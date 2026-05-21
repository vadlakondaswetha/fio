/*
 * fsspec IO engine
 *
 * IO engine that uses Python's fsspec library to access various filesystems.
 * Uses py_adapter to communicate with Python.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include "../fio.h"
#include "../optgroup.h"
#include "py_adapter.h"

/* Global shared filesystem handles for thread mode (thread=1) */
static PyFsHandle global_fs = NULL;
static pthread_mutex_t global_fs_mutex = PTHREAD_MUTEX_INITIALIZER;
static int global_fs_ref_count = 0;

struct fsspec_data {
	PyFsHandle fs;
	PyFileHandle *file_objs;
};

struct fsspec_io_u_data {
	void *memview;
	char *cached_buf;
	unsigned long long cached_len;
};

struct fsspec_options {
	void *pad;
	char *protocol;
	char *storage_options;
};

static struct fio_option options[] = {
	{
		.name     = "fsspec_protocol",
		.lname    = "fsspec protocol",
		.type     = FIO_OPT_STR_STORE,
		.help     = "fsspec protocol (e.g., s3, gcs, file, memory)",
		.off1     = offsetof(struct fsspec_options, protocol),
		.def      = "file",
		.category = FIO_OPT_C_ENGINE,
		.group    = FIO_OPT_G_INVALID,
	},
	{
		.name     = "fsspec_storage_options",
		.lname    = "fsspec storage options",
		.type     = FIO_OPT_STR_STORE,
		.help     = "Comma-separated key=value storage options for fsspec",
		.off1     = offsetof(struct fsspec_options, storage_options),
		.category = FIO_OPT_C_ENGINE,
		.group    = FIO_OPT_G_INVALID,
	},
	{
		.name     = NULL,
	},
};

static int fio_fsspec_init(struct thread_data *td) {
	struct fsspec_options *o = td->eo;
	struct fsspec_data *sd;

	if (py_adapter_init()) {
		return 1;
	}

	sd = calloc(1, sizeof(*sd));
	if (!sd) {
		return 1;
	}

	/* 
	 * Thread-Safe Shared Filesystem Initialization:
	 * If thread=1 is used, only the first thread creates the filesystem instance.
	 * All subsequent threads in the same process reuse the shared 'global_fs'.
	 * If thread=0 (process mode) is used, fork happened before init, so each child 
	 * process starts with global_fs = NULL and naturally creates its own instance.
	 */
	pthread_mutex_lock(&global_fs_mutex);
	if (!global_fs) {
		global_fs = py_adapter_create_filesystem(o->protocol, o->storage_options);
	}
	if (global_fs) {
		sd->fs = global_fs;
		global_fs_ref_count++;
	}
	pthread_mutex_unlock(&global_fs_mutex);

	if (!sd->fs) {
		free(sd);
		return 1;
	}

	sd->file_objs = calloc(td->o.nr_files, sizeof(PyFileHandle));
	if (!sd->file_objs) {
		pthread_mutex_lock(&global_fs_mutex);
		global_fs_ref_count--;
		if (global_fs_ref_count == 0) {
			py_adapter_free_filesystem(global_fs);
			global_fs = NULL;
		}
		pthread_mutex_unlock(&global_fs_mutex);
		free(sd);
		return 1;
	}

	td->io_ops_data = sd;
	return 0;
}

static int fio_fsspec_open(struct thread_data *td, struct fio_file *f) {
	struct fsspec_data *sd = td->io_ops_data;
	const char *mode = "rb";
	PyFileHandle file_obj;

	if (td_write(td)) {
		mode = "wb";
	}
	if (td_rw(td)) {
		mode = "r+b";
	}

	file_obj = py_adapter_open_file(sd->fs, f->file_name, mode);
	if (!file_obj) {
		return 1;
	}

	sd->file_objs[f->fileno] = file_obj;
	f->fd = f->fileno; 

	return 0;
}

static int fio_fsspec_close(struct thread_data *td, struct fio_file *f) {
	struct fsspec_data *sd = td->io_ops_data;
	PyFileHandle file_obj = sd->file_objs[f->fileno];

	if (file_obj) {
		py_adapter_close_file(file_obj);
		py_adapter_free_file(file_obj);
		sd->file_objs[f->fileno] = NULL;
	}

	return 0;
}

static void fio_fsspec_cleanup(struct thread_data *td) {
	struct fsspec_data *sd = td->io_ops_data;
	
	if (sd) {
		for (unsigned int i = 0; i < td->o.nr_files; i++) {
			if (sd->file_objs[i]) {
				py_adapter_free_file(sd->file_objs[i]);
			}
		}
		free(sd->file_objs);
		
		/* Thread-safe release of the shared filesystem instance */
		pthread_mutex_lock(&global_fs_mutex);
		global_fs_ref_count--;
		if (global_fs_ref_count == 0) {
			if (global_fs) {
				py_adapter_free_filesystem(global_fs);
				global_fs = NULL;
			}
		}
		pthread_mutex_unlock(&global_fs_mutex);

		free(sd);
		td->io_ops_data = NULL;
	}
	py_adapter_cleanup();
}

static int fio_fsspec_io_u_init(struct thread_data *td, struct io_u *io_u) {
	struct fsspec_io_u_data *iud = calloc(1, sizeof(*iud));
	if (!iud) {
		return 1;
	}
	io_u->engine_data = iud;
	return 0;
}

static void fio_fsspec_io_u_free(struct thread_data *td, struct io_u *io_u) {
	struct fsspec_io_u_data *iud = io_u->engine_data;
	if (iud) {
		if (iud->memview) {
			py_adapter_free_memoryview(iud->memview);
		}
		free(iud);
		io_u->engine_data = NULL;
	}
}

static int fio_fsspec_get_file_size(struct thread_data *td, struct fio_file *f) {
	struct fsspec_options *o = td->eo;
	PyFsHandle fs;
	long long size = -1;

	if (py_adapter_init()) {
		return 1;
	}

	fs = py_adapter_create_filesystem(o->protocol, o->storage_options);
	if (!fs) {
		return 1;
	}

	size = py_adapter_get_file_size(fs, f->file_name);
	
	py_adapter_free_filesystem(fs);
	
	if (size < 0) {
		return 1;
	}

	f->real_file_size = size;
	return 0;
}

static enum fio_q_status fio_fsspec_queue(struct thread_data *td,
					 struct io_u *io_u)
{
	struct fsspec_data *sd = td->io_ops_data;
	PyFileHandle file_obj = sd->file_objs[io_u->file->fileno];
	struct fsspec_io_u_data *iud = io_u->engine_data;

	fio_ro_check(td, io_u);

	if (py_adapter_seek(file_obj, io_u->offset) < 0) {
		io_u->error = EIO;
		return FIO_Q_COMPLETED;
	}

	/* Lazy initialization & safety check for buffer shifting (short I/O) */
	if (iud->cached_buf != io_u->xfer_buf || iud->cached_len != io_u->xfer_buflen) {
		if (iud->memview) {
			py_adapter_free_memoryview(iud->memview);
		}
		iud->memview = py_adapter_create_memoryview(io_u->xfer_buf, io_u->xfer_buflen);
		iud->cached_buf = io_u->xfer_buf;
		iud->cached_len = io_u->xfer_buflen;
	}

	if (io_u->ddir == DDIR_READ) {
		long r = py_adapter_read(file_obj, io_u->xfer_buf, io_u->xfer_buflen, iud->memview);
		if (r < 0) {
			io_u->error = EIO;
		} else {
			io_u->resid = io_u->xfer_buflen - r;
		}
	} else if (io_u->ddir == DDIR_WRITE) {
		long w = py_adapter_write(file_obj, io_u->xfer_buf, io_u->xfer_buflen, iud->memview);
		if (w < 0) {
			io_u->error = EIO;
		} else {
			io_u->resid = io_u->xfer_buflen - w;
		}
	} else {
		log_err("fsspec: unsupported ddir %d\n", io_u->ddir);
		io_u->error = EINVAL;
	}

	return FIO_Q_COMPLETED;
}

static struct io_u *fio_fsspec_event(struct thread_data *td, int event) {
	return NULL;
}

static int fio_fsspec_getevents(struct thread_data *td, unsigned int min,
	unsigned int max, const struct timespec *t) {
	return 0;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "fsspec",
	.version		= FIO_IOOPS_VERSION,
	.flags			= FIO_DISKLESSIO | FIO_SYNCIO,
	.init			= fio_fsspec_init,
	.queue			= fio_fsspec_queue,
	.getevents		= fio_fsspec_getevents,
	.event			= fio_fsspec_event,
	.cleanup		= fio_fsspec_cleanup,
	.open_file		= fio_fsspec_open,
	.close_file		= fio_fsspec_close,
	.get_file_size	= fio_fsspec_get_file_size,
	.io_u_init		= fio_fsspec_io_u_init,
	.io_u_free		= fio_fsspec_io_u_free,
	.options		= options,
	.option_struct_size	= sizeof(struct fsspec_options),
};

static void fio_init fio_fsspec_register(void) {
	register_ioengine(&ioengine);
}

static void fio_exit fio_fsspec_unregister(void) {
	unregister_ioengine(&ioengine);
}
