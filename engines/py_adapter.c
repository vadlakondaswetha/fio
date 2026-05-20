#include <Python.h>
#include <pthread.h>
#include <string.h>
#include "py_adapter.h"
#include "../fio.h"

static pthread_mutex_t py_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static int py_initialized = 0;
static PyThreadState *main_thread_state = NULL;

int py_adapter_init(void) {
	pthread_mutex_lock(&py_init_mutex);
	if (!py_initialized) {
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
	/* No-op. We let OS cleanup Python on exit to avoid thread teardown issues. */
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
	}

	PyGILState_Release(gstate);
	return (PyFileHandle)file_obj;
}

int py_adapter_close_file(PyFileHandle file) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *close_method = NULL;
	PyObject *res = NULL;
	int ret = 0;

	close_method = PyObject_GetAttrString((PyObject*)file, "close");
	res = PyObject_CallObject(close_method, NULL);
	if (!res) {
		PyErr_Print();
		ret = -1;
	}
	Py_XDECREF(res);
	Py_DECREF(close_method);

	PyGILState_Release(gstate);
	return ret;
}

void py_adapter_free_file(PyFileHandle file) {
	if (file) {
		PyGILState_STATE gstate = PyGILState_Ensure();
		Py_DECREF((PyObject*)file);
		PyGILState_Release(gstate);
	}
}

long long py_adapter_seek(PyFileHandle file, long long offset) {
	PyGILState_STATE gstate = PyGILState_Ensure();
  PyObject *seekable_method = NULL;
	PyObject *seekable_res = NULL;
	int seekable = 0;
	PyObject *seekable_method = NULL;
	PyObject *seekable_res = NULL;
	int seekable = 0;
	PyObject *seek_method = NULL;
	PyObject *py_offset = NULL;
	PyObject *args = NULL;
	PyObject *res = NULL;
	long long ret = 0;

	/* Check if the file object is seekable (GCS/S3 write streams are NOT seekable) */
  seekable_method = PyObject_GetAttrString((PyObject*)file, "seekable");
  if (seekable_method) {
  	seekable_res = PyObject_CallObject(seekable_method, NULL);
  	if (seekable_res) {
  		seekable = PyObject_IsTrue(seekable_res);
  		Py_DECREF(seekable_res);
  	}
  	Py_DECREF(seekable_method);
  } else {
  	PyErr_Clear();
  	seekable = 1; /* Fallback to trying seek if no seekable() method is present */
  }
  if (!seekable) {
  	PyGILState_Release(gstate);
  	return offset; /* Pretend the seek succeeded for unseekable streams (e.g. GCS write) */
  }

	seek_method = PyObject_GetAttrString((PyObject*)file, "seek");
	py_offset = PyLong_FromUnsignedLongLong(offset);
	args = PyTuple_Pack(1, py_offset);
	res = PyObject_CallObject(seek_method, args);
	Py_DECREF(py_offset);
	Py_DECREF(args);
	Py_DECREF(seek_method);

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

static long py_adapter_read_fallback(PyFileHandle file, char *buf, long len) {
	PyObject *read_method = NULL;
	PyObject *py_len = NULL;
	PyObject *args = NULL;
	PyObject *data = NULL;
	long ret = -1;

	read_method = PyObject_GetAttrString((PyObject*)file, "read");
	py_len = PyLong_FromUnsignedLong(len);
	args = PyTuple_Pack(1, py_len);
	data = PyObject_CallObject(read_method, args);
	Py_DECREF(py_len);
	Py_DECREF(args);
	Py_DECREF(read_method);

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

long py_adapter_read(PyFileHandle file, char *buf, long len) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *readinto_method = NULL;
	PyObject *py_memview = NULL;
	PyObject *args = NULL;
	PyObject *bytes_read = NULL;
	long ret = -1;

	readinto_method = PyObject_GetAttrString((PyObject*)file, "readinto");
	if (!readinto_method) {
		PyErr_Clear();
		ret = py_adapter_read_fallback(file, buf, len);
		PyGILState_Release(gstate);
		return ret;
	}

	/* Create a writable memoryview wrapping our C buffer (zero-copy) */
	py_memview = PyMemoryView_FromMemory(buf, len, PyBUF_WRITE);
	if (!py_memview) {
		PyErr_Print();
		Py_DECREF(readinto_method);
		PyGILState_Release(gstate);
		return -1;
	}

	args = PyTuple_Pack(1, py_memview);
	bytes_read = PyObject_CallObject(readinto_method, args);
	Py_DECREF(args);
	Py_DECREF(py_memview);
	Py_DECREF(readinto_method);

	if (!bytes_read) {
		PyErr_Print();
	} else {
		ret = PyLong_AsLong(bytes_read);
		Py_DECREF(bytes_read);
	}

	PyGILState_Release(gstate);
	return ret;
}

long py_adapter_write(PyFileHandle file, const char *buf, long len) {
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyObject *write_method = NULL;
	PyObject *py_memview = NULL;
	PyObject *args = NULL;
	PyObject *written = NULL;
	long ret = -1;

	write_method = PyObject_GetAttrString((PyObject*)file, "write");
	
	/* Create a read-only memoryview wrapping our C buffer (zero-copy) */
	py_memview = PyMemoryView_FromMemory((char*)buf, len, PyBUF_READ);
	if (!py_memview) {
		PyErr_Print();
		Py_DECREF(write_method);
		PyGILState_Release(gstate);
		return -1;
	}

	args = PyTuple_Pack(1, py_memview);
	written = PyObject_CallObject(write_method, args);
	Py_DECREF(args);
	Py_DECREF(py_memview);
	Py_DECREF(write_method);

	if (!written) {
		PyErr_Print();
	} else {
		ret = PyLong_AsLong(written);
		Py_DECREF(written);
	}

	PyGILState_Release(gstate);
	return ret;
}
