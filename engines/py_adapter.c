#include <Python.h>
#include <pthread.h>
#include <string.h>
#include "py_adapter.h"
#include "../fio.h"

static pthread_mutex_t py_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static int py_initialized = 0;
static PyThreadState *main_thread_state = NULL;

typedef struct {
	PyObject *file_obj;
	PyObject *readinto_method;
	PyObject *read_fallback_method;
	PyObject *write_method;
	PyObject *seek_method;
	int seekable;
} PyFile;

int py_adapter_init(void) {
	pthread_mutex_lock(&py_init_mutex);
	if (!py_initialized) {
		/* Set high-performance tuning environment variables for GCSFS */
		setenv("USE_EXPERIMENTAL_ADAPTIVE_PREFETCHING", "true", 1);
		setenv("DEFAULT_GCSFS_CONCURRENCY", "4", 1);

		Py_Initialize();
#if PY_VERSION_HEX < 0x03090000
		PyEval_InitThreads();
#endif
		main_thread_state = PyEval_SaveThread();
		py_initialized = 1;
	}
	pthread_mutex_unlock(&py_init_mutex);
	return 0;
}

void py_adapter_cleanup(void) {
	/* No-op. */
}

PyFsHandle py_adapter_create_filesystem(const char *protocol, const char *storage_options) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *fsspec_module = NULL;
	PyObject *kwargs = NULL;
	PyObject *filesystem_func = NULL;
	PyObject *py_protocol = NULL;
	PyObject *args = NULL;
	PyObject *fs = NULL;

	fsspec_module = PyImport_ImportModule("fsspec");
	if (!fsspec_module) {
		log_err("fsspec: failed to import fsspec module.\n");
		PyErr_Print();
		PyGILState_Release(gstate);
		return NULL;
	}

	kwargs = PyDict_New();
	if (storage_options) {
		char *opts = strdup(storage_options);
		char *token = strtok(opts, ",");
		while (token) {
			char *eq = strchr(token, '=');
			if (eq) {
				char *key = token;
				char *val = eq + 1;
				PyObject *py_val;

				*eq = '\0';
				py_val = PyUnicode_FromString(val);
				PyDict_SetItemString(kwargs, key, py_val);
				Py_DECREF(py_val);
			}
			token = strtok(NULL, ",");
		}
		free(opts);
	}

	filesystem_func = PyObject_GetAttrString(fsspec_module, "filesystem");
	py_protocol = PyUnicode_FromString(protocol);
	args = PyTuple_Pack(1, py_protocol);
	
	fs = PyObject_Call(filesystem_func, args, kwargs);
	Py_DECREF(py_protocol);
	Py_DECREF(args);
	Py_DECREF(kwargs);
	Py_DECREF(filesystem_func);
	Py_DECREF(fsspec_module);

	if (!fs) {
		log_err("fsspec: failed to create filesystem for protocol '%s'\n", protocol);
		PyErr_Print();
	}

	PyGILState_Release(gstate);
	return (PyFsHandle)fs;
}

void py_adapter_free_filesystem(PyFsHandle fs) {
	if (fs) {
		PyGILState_STATE gstate = PyGILState_Ensure();
		Py_DECREF((PyObject*)fs);
		PyGILState_Release(gstate);
	}
}

PyFileHandle py_adapter_open_file(PyFsHandle fs, const char *path, const char *mode) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *open_method = NULL;
	PyObject *py_filename = NULL;
	PyObject *py_mode = NULL;
	PyObject *args = NULL;
	PyObject *file_obj = NULL;
	PyFile *py_file = NULL;
	PyObject *seekable_method = NULL;
	PyObject *seekable_res = NULL;

	open_method = PyObject_GetAttrString((PyObject*)fs, "open");
	py_filename = PyUnicode_FromString(path);
	py_mode = PyUnicode_FromString(mode);
	args = PyTuple_Pack(2, py_filename, py_mode);
	file_obj = PyObject_CallObject(open_method, args);
	Py_DECREF(py_filename);
	Py_DECREF(py_mode);
	Py_DECREF(args);
	Py_DECREF(open_method);

	if (!file_obj) {
		log_err("fsspec: failed to open file '%s' with mode '%s'\n", path, mode);
		PyErr_Print();
		PyGILState_Release(gstate);
		return NULL;
	}

	py_file = calloc(1, sizeof(PyFile));
	if (!py_file) {
		Py_DECREF(file_obj);
		PyGILState_Release(gstate);
		return NULL;
	}

	py_file->file_obj = file_obj;
	
	/* Cache methods to avoid attribute lookup on every I/O */
	py_file->readinto_method = PyObject_GetAttrString(file_obj, "readinto");
	if (!py_file->readinto_method) {
		PyErr_Clear();
	}
	py_file->read_fallback_method = PyObject_GetAttrString(file_obj, "read");
	if (!py_file->read_fallback_method) {
		PyErr_Clear();
	}
	py_file->write_method = PyObject_GetAttrString(file_obj, "write");
	if (!py_file->write_method) {
		PyErr_Clear();
	}
	py_file->seek_method = PyObject_GetAttrString(file_obj, "seek");
	if (!py_file->seek_method) {
		PyErr_Clear();
	}

	/* Cache seekable status */
	seekable_method = PyObject_GetAttrString(file_obj, "seekable");
	if (seekable_method) {
		seekable_res = PyObject_CallObject(seekable_method, NULL);
		if (seekable_res) {
			py_file->seekable = PyObject_IsTrue(seekable_res);
			Py_DECREF(seekable_res);
		} else {
			PyErr_Clear();
			py_file->seekable = 1;
		}
		Py_DECREF(seekable_method);
	} else {
		PyErr_Clear();
		py_file->seekable = 1;
	}

	PyGILState_Release(gstate);
	return (PyFileHandle)py_file;
}

int py_adapter_close_file(PyFileHandle file) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyFile *py_file = (PyFile*)file;
	PyObject *res = NULL;
	int ret = 0;

	if (py_file && py_file->file_obj) {
		PyObject *close_method = PyObject_GetAttrString(py_file->file_obj, "close");
		if (close_method) {
			res = PyObject_CallObject(close_method, NULL);
			if (!res) {
				PyErr_Print();
				ret = -1;
			}
			Py_XDECREF(res);
			Py_DECREF(close_method);
		}
	}

	PyGILState_Release(gstate);
	return ret;
}

void py_adapter_free_file(PyFileHandle file) {
	if (file) {
		PyGILState_STATE gstate = PyGILState_Ensure();
		PyFile *py_file = (PyFile*)file;
		Py_XDECREF(py_file->readinto_method);
		Py_XDECREF(py_file->read_fallback_method);
		Py_XDECREF(py_file->write_method);
		Py_XDECREF(py_file->seek_method);
		Py_XDECREF(py_file->file_obj);
		free(py_file);
		PyGILState_Release(gstate);
	}
}

long long py_adapter_seek(PyFileHandle file, long long offset) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyFile *py_file = (PyFile*)file;
	PyObject *py_offset = NULL;
	PyObject *res = NULL;
	long long ret = 0;

	if (!py_file->seekable) {
		PyGILState_Release(gstate);
		return offset;
	}

	if (!py_file->seek_method) {
		PyGILState_Release(gstate);
		return -1;
	}

	py_offset = PyLong_FromUnsignedLongLong(offset);
	res = PyObject_CallFunctionObjArgs(py_file->seek_method, py_offset, NULL);
	Py_DECREF(py_offset);

	if (!res) {
		PyErr_Print();
		ret = -1;
	} else {
		if (res == Py_None) {
			ret = offset;
		} else {
			ret = PyLong_AsLongLong(res);
		}
	}
	Py_XDECREF(res);

	PyGILState_Release(gstate);
	return ret;
}

static long py_adapter_read_fallback(PyFile *py_file, char *buf, long len) {
	PyObject *py_len = NULL;
	PyObject *data = NULL;
	long ret = -1;

	if (!py_file->read_fallback_method) {
		return -1;
	}

	py_len = PyLong_FromUnsignedLong(len);
	data = PyObject_CallFunctionObjArgs(py_file->read_fallback_method, py_len, NULL);
	Py_DECREF(py_len);

	if (!data) {
		PyErr_Print();
	} else {
		char *py_buf;
		Py_ssize_t py_buf_len;
		if (PyBytes_AsStringAndSize(data, &py_buf, &py_buf_len) < 0) {
			PyErr_Print();
		} else {
			memcpy(buf, py_buf, py_buf_len);
			ret = (long)py_buf_len;
		}
		Py_DECREF(data);
	}
	return ret;
}

long py_adapter_read(PyFileHandle file, char *buf, long len, void *memview) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyFile *py_file = (PyFile*)file;
	PyObject *py_memview = (PyObject*)memview;
	PyObject *bytes_read = NULL;
	long ret = -1;
	int free_memview = 0;

	if (!py_file->readinto_method) {
		ret = py_adapter_read_fallback(py_file, buf, len);
		PyGILState_Release(gstate);
		return ret;
	}

	/* Use pre-allocated memoryview if provided, otherwise allocate a temporary one */
	if (!py_memview) {
		py_memview = PyMemoryView_FromMemory(buf, len, PyBUF_WRITE);
		if (!py_memview) {
			PyErr_Print();
			PyGILState_Release(gstate);
			return -1;
		}
		free_memview = 1;
	}

	/* Call using optimized CallFunctionObjArgs to avoid tuple allocation */
	bytes_read = PyObject_CallFunctionObjArgs(py_file->readinto_method, py_memview, NULL);
	
	if (free_memview) {
		Py_DECREF(py_memview);
	}

	if (!bytes_read) {
		PyErr_Print();
		/* Try fallback if readinto failed unexpectedly */
		ret = py_adapter_read_fallback(py_file, buf, len);
	} else {
		ret = PyLong_AsLong(bytes_read);
		Py_DECREF(bytes_read);
	}

	PyGILState_Release(gstate);
	return ret;
}

long py_adapter_write(PyFileHandle file, const char *buf, long len, void *memview) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyFile *py_file = (PyFile*)file;
	PyObject *py_memview = (PyObject*)memview;
	PyObject *written = NULL;
	long ret = -1;
	int free_memview = 0;

	if (!py_file->write_method) {
		PyGILState_Release(gstate);
		return -1;
	}

	if (!py_memview) {
		py_memview = PyMemoryView_FromMemory((char*)buf, len, PyBUF_READ);
		if (!py_memview) {
			PyErr_Print();
			PyGILState_Release(gstate);
			return -1;
		}
		free_memview = 1;
	}

	written = PyObject_CallFunctionObjArgs(py_file->write_method, py_memview, NULL);
	
	if (free_memview) {
		Py_DECREF(py_memview);
	}

	if (!written) {
		PyErr_Print();
	} else {
		ret = PyLong_AsLong(written);
		Py_DECREF(written);
	}

	PyGILState_Release(gstate);
	return ret;
}

void* py_adapter_create_memoryview(char *buf, long len) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *memview = PyMemoryView_FromMemory(buf, len, PyBUF_WRITE);
	if (!memview) {
		PyErr_Print();
	}
	PyGILState_Release(gstate);
	return (void*)memview;
}

void py_adapter_free_memoryview(void *memview) {
	if (memview) {
		PyGILState_STATE gstate = PyGILState_Ensure();
		Py_DECREF((PyObject*)memview);
		PyGILState_Release(gstate);
	}
}

long long py_adapter_get_file_size(PyFsHandle fs, const char *path) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *size_method = NULL;
	PyObject *py_filename = NULL;
	PyObject *args = NULL;
	PyObject *res = NULL;
	PyObject *info_method = NULL;
	PyObject *info_dict = NULL;
	long long ret = -1;

	size_method = PyObject_GetAttrString((PyObject*)fs, "size");
	if (!size_method) {
		PyErr_Clear();
		info_method = PyObject_GetAttrString((PyObject*)fs, "info");
		if (info_method) {
			py_filename = PyUnicode_FromString(path);
			args = PyTuple_Pack(1, py_filename);
			info_dict = PyObject_CallObject(info_method, args);
			Py_DECREF(py_filename);
			Py_DECREF(args);
			Py_DECREF(info_method);
			if (info_dict && PyDict_Check(info_dict)) {
				PyObject *py_size = PyDict_GetItemString(info_dict, "size");
				if (py_size) {
					ret = PyLong_AsLongLong(py_size);
				}
				Py_DECREF(info_dict);
			} else {
				PyErr_Print();
			}
		} else {
			PyErr_Print();
		}
	} else {
		py_filename = PyUnicode_FromString(path);
		args = PyTuple_Pack(1, py_filename);
		res = PyObject_CallObject(size_method, args);
		Py_DECREF(py_filename);
		Py_DECREF(args);
		Py_DECREF(size_method);

		if (!res) {
			PyErr_Print();
		} else {
			ret = PyLong_AsLongLong(res);
			Py_DECREF(res);
		}
	}

	PyGILState_Release(gstate);
	return ret;
}
