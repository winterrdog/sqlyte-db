/*
    MIT License

    Copyright (c) 2024 winterrdog

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "xmem.h"

#define STRINGS_EQUAL(a, b, len_b) strncmp(a, b, len_b) == 0
#define PRINT_TO_STDERR(fmt, ...) fprintf(stderr, fmt, __VA_ARGS__)
#define PRINT_TO_STDERR_NOARGS(fmt) fprintf(stderr, fmt)

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
    PREPARE_UNRECOGNIZED_STATEMENT
} prepare_result_t;

// type for all actual SQL statements used in our SQL database
// e.g. SELECT or INSERT
typedef enum {
    STATEMENT_INSERT = 0,
    STATEMENT_SELECT,
} statement_t;
typedef struct {
    statement_t type;
} statement;

// function prototypes
void close_input_buffer(input_buffer_t* in);

input_buffer_t* new_input_buffer(void)
{
    input_buffer_t* ret;

    ret = xmalloc(sizeof(input_buffer_t));
    ret->buf = NULL, ret->input_len = 0, ret->buf_len = 0;

    return ret;
}

meta_cmd_result_t exec_meta_cmd(input_buffer_t* in)
{
    const char* exit_cmd = ".exit";
    if (STRINGS_EQUAL(exit_cmd, in->buf, strlen(in->buf))) {
        close_input_buffer(in);
        exit(EXIT_SUCCESS);
    }

    return META_CMD_UNRECOGNIZED_CMD;
}

prepare_result_t prepare_statement(input_buffer_t* in, statement* st)
{
    const char* st_insert = "insert";
    const char* st_select = "select";
    if (STRINGS_EQUAL(st_insert, in->buf, strlen(in->buf))) {
        st->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (STRINGS_EQUAL(st_select, in->buf, strlen(in->buf))) {
        st->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void exec_statement(statement* st)
{
    switch (st->type) {
    case STATEMENT_INSERT:
        printf("todo: make an insert\n");
        break;
    case STATEMENT_SELECT:
        printf("todo: make a select\n");
        break;
    }
}

void print_prompt(void)
{
    printf("lyt-db > ");
}

int read_input(input_buffer_t* in)
{
    ssize_t bytes_read = getline(&in->buf, &in->buf_len, stdin);
    if (bytes_read < 0) {
        return -1;
    }

    in->input_len = bytes_read - 1;

    // remove trailing line
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
            switch (exec_meta_cmd(user_input)) {
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
        case PREPARE_UNRECOGNIZED_STATEMENT:
            PRINT_TO_STDERR("unrecognized keyword at start of '%s'.\n", user_input->buf);
            continue;
        }
        exec_statement(&st);
        printf("executed.\n");
    } while (1);
}

int main(int argc, char* argv[])
{
    run_repl();
    return EXIT_SUCCESS;
}
