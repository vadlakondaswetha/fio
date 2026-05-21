#ifndef PY_ADAPTER_H
#define PY_ADAPTER_H

typedef void* PyFsHandle;
typedef void* PyFileHandle;

int py_adapter_init(void);
void py_adapter_cleanup(void);

PyFsHandle py_adapter_create_filesystem(const char *protocol, const char *storage_options);
void py_adapter_free_filesystem(PyFsHandle fs);

PyFileHandle py_adapter_open_file(PyFsHandle fs, const char *path, const char *mode);
int py_adapter_close_file(PyFileHandle file);
void py_adapter_free_file(PyFileHandle file);

long long py_adapter_seek(PyFileHandle file, long long offset);
long py_adapter_read(PyFileHandle file, char *buf, long len, void *memview);
long py_adapter_write(PyFileHandle file, const char *buf, long len, void *memview);

void* py_adapter_create_memoryview(char *buf, long len);
void py_adapter_free_memoryview(void *memview);

long long py_adapter_get_file_size(PyFsHandle fs, const char *path);

#endif
