/*
 * A small header than can be included by backported LLVM code or PostgreSQL
 * code, to control conditional compilation.
 */
#ifndef LLVMJIT_BACKPORT_H
#define LLVMJIT_BACKPORT_H

#include <llvm/Config/llvm-config.h>

/*
 * LLVM RuntimeDyld can produce code that crashes on larger memory ARM systems,
 * due to memory placement.  As a workaround, we supply an alternative memory
 * manager class from https://github.com/llvm/llvm-project/pull/71968, but
 * we've only backported it as far as LLVM 12.
 */
#if defined(__aarch64__) && LLVM_VERSION_MAJOR > 11
#define USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
#endif

#endif
