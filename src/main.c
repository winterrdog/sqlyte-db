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
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
typedef enum {
    EXECUTE_SUCCESS = 0,
    EXECUTE_TABLE_FULL
} execute_result_t;

// type for all actual SQL statements used in our SQL database
// e.g. SELECT or INSERT
typedef enum {
    STATEMENT_INSERT = 0,
    STATEMENT_SELECT
} statement_t;
typedef struct {
    statement_t type;
    row_t row_to_insert; // only used by "INSERT"
} statement;

// table data structure layout
const u32 PAGE_SIZE = 4096;
const u32 ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const u32 TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
typedef struct {
    u32 row_count;
    void* pages[TABLE_MAX_PAGES];
} table_t;

// function prototypes
void close_input_buffer(input_buffer_t* in);

void print_row(row_t* r)
{
    printf("( %d, %s, %s )\n", r->id, r->username, r->email);
}

void serialize_row(row_t* src, void* dest)
{
    // store id
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);

    // store username
    memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);

    // store email
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void* src, row_t* dest)
{
    // read id
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);

    // read username
    memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);

    // read email
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot(table_t* table, u32 row_num)
{
    u32 page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if (!page) {
        // allocate memory only wen we try to access the page
        page = xmalloc(PAGE_SIZE);
        table->pages[page_num] = page;
    }
    u32 row_offset = row_num % ROWS_PER_PAGE;
    u32 byte_offset = row_offset * ROW_SIZE;

    return page + byte_offset;
}

table_t* new_table()
{
    table_t* table;
    u32 i;

    table = xmalloc(sizeof(table_t));
    table->row_count = 0;
    for (i = 0; i != TABLE_MAX_PAGES; ++i) {
        table->pages[i] = NULL;
    }

    return table;
}

void free_table(table_t* t)
{
    for (u32 i = 0; i != TABLE_MAX_PAGES; ++i) {
        if (t->pages[i]) {
            xfree(t->pages[i]);
        }
    }
    xfree(t);
}

input_buffer_t* new_input_buffer(void)
{
    input_buffer_t* ret;

    ret = xmalloc(sizeof(input_buffer_t));
    ret->buf = NULL, ret->input_len = 0, ret->buf_len = 0;

    return ret;
}

u8 str_exactly_equal(const char* s1, const char* s2)
{
    size_t len1 = strlen(s1);
    size_t len2 = strlen(s2);

    return len1 == len2 && (strncmp(s1, s2, len1) == 0);
}

meta_cmd_result_t exec_meta_cmd(input_buffer_t* in, table_t* t)
{
    const char* exit_cmd = ".exit";
    if (str_exactly_equal(exit_cmd, in->buf)) {
        close_input_buffer(in);
        free_table(t);
        exit(EXIT_SUCCESS);
    }

    return META_CMD_UNRECOGNIZED_CMD;
}

prepare_result_t prepare_insert(input_buffer_t* in, statement* st)
{
    st->type = STATEMENT_INSERT;

    char* keyword = strtok(in->buf, " ");
    char* curr_id = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    if (!curr_id || !username || !email) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(curr_id);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    st->row_to_insert.id = id,
    strcpy(st->row_to_insert.username, username),
    strcpy(st->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

prepare_result_t prepare_statement(input_buffer_t* in, statement* st)
{
    const char* st_insert = "insert";
    const char* st_select = "select";
    if (!strncmp(st_insert, in->buf, strlen(st_insert))) {
        return prepare_insert(in, st);
    }
    if (!strcmp(st_select, in->buf)) {
        st->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

execute_result_t exec_insert(statement* st, table_t* t)
{
    if (t->row_count >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    row_t* new_row = &(st->row_to_insert);
    serialize_row(new_row, row_slot(t, t->row_count));
    t->row_count++;

    return EXECUTE_SUCCESS;
}

execute_result_t exec_select(statement* st, table_t* t)
{
    row_t r;
    for (u32 i = 0; i != t->row_count; ++i) {
        deserialize_row(row_slot(t, i), &r);
        print_row(&r);
    }

    return EXECUTE_SUCCESS;
}

execute_result_t exec_statement(statement* st, table_t* t)
{
    switch (st->type) {
    case STATEMENT_INSERT:
        return exec_insert(st, t);
    case STATEMENT_SELECT:
        return exec_select(st, t);
    }
}

void print_prompt(void) { printf("lyt-db> "); }

int read_input(input_buffer_t* in)
{
    ssize_t bytes_read = getline(&in->buf, &in->buf_len, stdin);
    if (bytes_read < 0) {
        return -1;
    }

    in->input_len = bytes_read - 1;

    // remove trailing new-line feed
    in->buf[bytes_read - 1] = 0;

    return 0;
}

void close_input_buffer(input_buffer_t* in)
{
    xfree(in->buf);
    xfree(in);
}

void run_repl(void)
{
    table_t* table = new_table();
    input_buffer_t* user_input = new_input_buffer();

    // make a REPL
    do {
        print_prompt();
        int ret = read_input(user_input);
        if (ret < 0) {
            return;
        }

        if (!user_input->buf[0]) {
            continue;
        }

        // is it a meta command
        if (user_input->buf[0] == '.') {
            switch (exec_meta_cmd(user_input, table)) {
            case META_CMD_SUCCESS:
                continue;
            case META_CMD_UNRECOGNIZED_CMD:
                PRINT_TO_STDERR("unrecognized command '%s'\n", user_input->buf);
                continue;
            }
        }

        // process the statement
        statement st;
        switch (prepare_statement(user_input, &st)) {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_SYNTAX_ERROR:
            PRINT_TO_STDERR_NOARGS("syntax error. could not parse statement.\n");
            continue;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf(
                "unrecognized keyword at start of '%s'.\n", user_input->buf);
            continue;
        case PREPARE_NEGATIVE_ID:
            printf("id must be non-negative.\n");
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("string is too long.\n");
            continue;
        }

        switch (exec_statement(&st, table)) {
        case EXECUTE_SUCCESS:
            printf("executed.\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("error: table's full.\n");
            break;
        }
    } while (1);
}

int main(int argc, char* argv[])
{
    run_repl();
    return EXIT_SUCCESS;
}
