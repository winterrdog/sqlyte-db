/*
    MIT License

    Originally developed by Robin Gareus <robin@gareus.org>

    Copyright 2010 Robin Gareus <robin@gareus.org>

    Copyright (c) 2024 winterrdog

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to
   deal in the Software without restriction, including without limitation the
   rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
   sell copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
   all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
   FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
   IN THE SOFTWARE.

    - winterrdog: readapted xstrdup()
    - winterrdog: added xfree()
    - winterrdog: added xfree_all()

*/

#ifndef _OAUTH_XMALLOC_H
#define _OAUTH_XMALLOC_H 1

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

/* Prototypes for functions */
// void* xmalloc(size_t size);
// void* xcalloc(size_t nmemb, size_t size);
// void* xrealloc(void* ptr, size_t size);
// void xfree(void* p);
// void xfree_all(void);
// void free_mem(void* p);
// const char* xstrdup(const char* s);

/*
** Header on the linked list of memory allocations.
*/
typedef struct mem_blk_hdr_t mem_blk_hdr_t;
struct mem_blk_hdr_t {
    mem_blk_hdr_t* next_blk;
    size_t blk_size;
    /* actual memory block comes next, right here! */
};

/*
** Global "singly-linked" list of all memory allocations.
*/
static mem_blk_hdr_t* memory_blks_list = NULL;

/*
** Wrappers around malloc(), calloc(), realloc() and free().
**
** All memory allocations are kept on a SINGLY-linked list.  The
** xfree_all() function can be called prior to exit to clean
** up any memory leaks.
**
** This is not needed but compilers and getting increasingly
** noisy about memory leaks. So this code is provided to hush them
** warnings.
*/
static void* xmalloc_fatal(size_t size)
{
    if (size == 0) {
        return NULL;
    }

    fprintf(stderr, "Out of memory. Failed to allocate %zd bytes. exiting..!\n", size);
    exit(1);
}

static inline mem_blk_hdr_t* get_mem_block_header(void* ptr)
{
    return (((mem_blk_hdr_t*)ptr) - 1);
}

static inline void* get_mem_block_data(mem_blk_hdr_t* blk_hdr)
{
    return (void*)(blk_hdr + 1);
}

static void free_mem(void* ptr)
{
    if (ptr) {
        free(ptr);
        ptr = NULL;
    }
}

static void* xmalloc(size_t size)
{
    mem_blk_hdr_t* new_blk;
    size_t needed_space;

    if (size < 0) {
        return NULL;
    }

    needed_space = size + sizeof(mem_blk_hdr_t);
    new_blk = (mem_blk_hdr_t*)malloc(needed_space);
    if (!new_blk) {
        return xmalloc_fatal(size);
    }

    new_blk->next_blk = memory_blks_list;
    new_blk->blk_size = size;
    memory_blks_list = new_blk;

    return get_mem_block_data(new_blk);
}

static void* xcalloc(size_t nmemb, size_t size)
{
    void* new_blk;
    size_t needed_space;

    needed_space = nmemb * size;
    new_blk = xmalloc(needed_space);
    memset(new_blk, 0x0, needed_space);

    return new_blk;
}

static void* xrealloc(void* old_ptr, size_t size)
{
    void* new_blk;
    mem_blk_hdr_t* blk_hdr;

    if (!old_ptr) {
        return xmalloc(size);
    }

    blk_hdr = get_mem_block_header(old_ptr);
    if (blk_hdr->blk_size >= size) {
        return old_ptr;
    }

    new_blk = xmalloc(size);
    memcpy(new_blk, old_ptr, blk_hdr->blk_size);

    return new_blk;
}

static const char* xstrdup(const char* src)
{
    size_t src_len = strlen(src) + 1;

    char* dest = (char*)xmalloc(src_len);
    memcpy(dest, src, src_len);
    dest[src_len - 1] = '\0';

    return (const char*)dest;
}

// make sure u free memory allocated by xmalloc, xcalloc, xrealloc
static void xfree(void* old_blk)
{
    if (old_blk) {
        mem_blk_hdr_t* blk_hdr = get_mem_block_header(old_blk);
        memset(old_blk, 0, blk_hdr->blk_size);
    }
}

static void xfree_all(void)
{
    for (mem_blk_hdr_t* next_blk = NULL; memory_blks_list != NULL;) {
        next_blk = memory_blks_list->next_blk;
        free_mem(memory_blks_list);

        memory_blks_list = next_blk;
    }
}

#endif