/*
    MIT License

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
*/
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
//
#include "xmem.h"

#define PRINT_TO_STDERR(fmt, ...) fprintf(stderr, fmt, __VA_ARGS__)
#define PRINT_TO_STDERR_NOARGS(fmt) fprintf(stderr, fmt)
#define SIZE_OF_ATTRIBUTE(_struct, _attr) sizeof(((_struct*)0x0)->_attr)

// hardcoded schema limits
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

// table-related
#define TABLE_MAX_PAGES 100

typedef unsigned int u32;
typedef unsigned char u8;

/*
    used by database to interact with filesystem and memory
*/
typedef struct {
    int fd;
    u32 file_len;
    u32 num_pages;
    void* pages[TABLE_MAX_PAGES];
} pager_t;

/*
    hard-coded schema type/shape
*/
typedef struct {
    u32 id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} row_t;

// schema field sizes
const u32 ID_SIZE = SIZE_OF_ATTRIBUTE(row_t, id);
const u32 USERNAME_SIZE = SIZE_OF_ATTRIBUTE(row_t, username);
const u32 EMAIL_SIZE = SIZE_OF_ATTRIBUTE(row_t, email);
const u32 ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // raw row size

// schema field offsets
const u32 ID_OFFSET = 0x0;
const u32 USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const u32 EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

/*
    struct to keep state of the user input from 'getline'
*/
typedef struct {
    char* buf;
    size_t buf_len;
    ssize_t input_len;
} input_buffer_t;

/*
    types to track the state of meta commands used in our SQL database
*/
// enum to handle meta commands i.e. cmds that are not SQL but
// help with controlling the database engine like ".exit"
typedef enum {
    META_CMD_SUCCESS = 0,
    META_CMD_UNRECOGNIZED_CMD
} meta_cmd_result_t;

/*
    types to track the state of SQL statements in SQL database
*/
typedef enum {
    PREPARE_SUCCESS = 0,
    PREPARE_SYNTAX_ERROR,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT
} prepare_result_t;
typedef enum { EXECUTE_SUCCESS = 0, EXECUTE_TABLE_FULL } execute_result_t;

// type for all actual SQL statements used in our SQL database
// e.g. SELECT or INSERT
typedef enum { STATEMENT_INSERT = 0, STATEMENT_SELECT } statement_t;
typedef struct {
    statement_t type;
    row_t row_to_insert; // only used by "INSERT"
} statement;

// table data structure layout
const u32 PAGE_SIZE = 4096;
const u32 ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const u32 TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
typedef struct {
    u32 root_page_num;
    pager_t* pager;
} table_t;
typedef struct {
    table_t* table;
    u32 page_num;
    u32 cell_num;
    u8 end_of_table; // indicates a position one past the last element
} cursor_t;

// B -T R E E
// S P E C I F I C S //

typedef enum { NODE_INTERNAL, NODE_LEAF } node_type_t;

// node header layout
const u32 NODE_TYPE_SIZE = sizeof(u8);
const u32 IS_ROOT_SIZE = sizeof(u8);
const u32 PARENT_POINTER_SIZE = sizeof(u32);
const u8 COMMON_NODE_HEADER_SIZE
    = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// offsets
const u32 NODE_TYPE_OFFSET = 0x0;
const u32 IS_ROOT_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const u32 PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;

// leaf node header layout
const u32 LEAF_NODE_NUM_CELLS_SIZE = sizeof(u32);
const u32 LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const u32 LEAF_NODE_HEADER_SIZE
    = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// leaf node body layout
const u32 LEAF_NODE_KEY_SIZE = sizeof(u32);
const u32 LEAF_NODE_KEY_OFFSET = 0x0;
const u32 LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const u32 LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const u32 LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const u32 LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const u32 LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// E N D
// O F
// B - T R E E

// function prototypes
void close_input_buffer(input_buffer_t* in);
void print_row(row_t* r);
void* get_page(pager_t* pager, u32 page_num);
void serialize_row(row_t* src, void* dest);
void deserialize_row(void* src, row_t* dest);
void print_prompt(void);
int read_input(input_buffer_t* in);
void run_repl(const char* fname);
void cursor_advance(cursor_t* c);
void* cursor_value(cursor_t* c);
void pager_flush(pager_t* pager, u32 page_num);
void db_close(table_t* t);
cursor_t* table_start(table_t* t);
cursor_t* table_end(table_t* t);
pager_t* pager_open(const char* fname);
table_t* db_open(const char* fname);
input_buffer_t* new_input_buffer(void);
u8 str_exactly_equal(const char* s1, const char* s2);
meta_cmd_result_t exec_meta_cmd(input_buffer_t* in, table_t* t);
prepare_result_t prepare_insert(input_buffer_t* in, statement* st);
prepare_result_t prepare_statement(input_buffer_t* in, statement* st);
execute_result_t exec_insert(statement* st, table_t* t);
execute_result_t exec_select(statement* st, table_t* t);
execute_result_t exec_statement(statement* st, table_t* t);
void print_constants(void);

// access leaf node fields
void initialize_leaf_node(void* node);
u32* leaf_node_num_cells(void* node);
void* leaf_node_cell(void* node, u32 cell_num);
u32* leaf_node_key(void* node, u32 cell_num);
void* leaf_node_value(void* node, u32 cell_num);
void leaf_node_insert(cursor_t* c, u32 key, row_t* value);
