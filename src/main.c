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
#include "db.h"
#include "xmem.h"

void print_row(row_t* r)
{
    printf("( %d, %s, %s )\n", r->id, r->username, r->email);
}

void* get_page(pager_t* pager, u32 page_num)
{
    if (page_num > TABLE_MAX_PAGES) {
        printf("tried to fetch a page out of bounds. %d > %d\n", page_num,
            TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (!pager->pages[page_num]) {
        // cache miss. allocate memory and load from file
        void* page = xmalloc(PAGE_SIZE);
        u32 num_pages = pager->file_len / PAGE_SIZE;

        // we might hv saved a partial page at the end of file
        if (pager->file_len % PAGE_SIZE != 0) {
            num_pages++;
        }

        // fetch page from file
        if (page_num <= num_pages) {
            off_t off = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
            if (off < 0) {
                printf("failed to reposition for the current page.\n");
                exit(EXIT_FAILURE);
            }
            ssize_t bytes_read = read(pager->fd, page, PAGE_SIZE);
            if (bytes_read < 0) {
                printf("failed to read in data from file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
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

cursor_t* table_start(table_t* table)
{
    cursor_t* cursor;

    cursor = xmalloc(sizeof(cursor_t));
    cursor->table = table, cursor->row_num = 0,
    cursor->end_of_table = (table->row_count == 0);

    return cursor;
}

cursor_t* table_end(table_t* t)
{
    cursor_t* cursor;

    cursor = xmalloc(sizeof(cursor_t));
    cursor->table = t, cursor->row_num = t->row_count,
    cursor->end_of_table = 0x1;

    return cursor;
}

void* cursor_value(cursor_t* c)
{
    u32 row_num, page_num, row_offset, byte_offset;
    void* page;

    row_num = c->row_num;
    page_num = row_num / ROWS_PER_PAGE;
    row_offset = row_num % ROWS_PER_PAGE;
    byte_offset = row_offset * ROW_SIZE;
    page = get_page(c->table->pager, page_num);

    return page + byte_offset;
}

void cursor_advance(cursor_t* c)
{
    c->row_num++;
    if (c->row_num < c->table->row_count) {
        return;
    }
    c->end_of_table = 0x1;
}

pager_t* pager_open(const char* fname)
{
    // open file in r/w mode or creating one if not existent
    // with read & write permissions for current user
    int fd = open(fname, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        printf("unable to open file, %d.\n", errno);
        exit(EXIT_FAILURE);
    }

    // set up pager
    off_t file_len = lseek(fd, 0, SEEK_END);
    pager_t* pager = xmalloc(sizeof(pager_t));
    pager->file_len = file_len, pager->fd = fd;
    for (u32 i = 0; i != TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }

    return pager;
}

table_t* db_open(const char* fname)
{
    table_t* table;
    pager_t* pager;
    u32 row_count;

    pager = pager_open(fname);
    row_count = pager->file_len / ROW_SIZE;
    table = xmalloc(sizeof(table_t));
    table->row_count = row_count, table->pager = pager;

    return table;
}

void pager_flush(pager_t* pager, u32 page_num, u32 size)
{
    if (!pager->pages[page_num]) {
        printf("tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
    if (offset < 0) {
        printf("failed to reposition for the current page, %d.\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->fd, pager->pages[page_num], size);
    if (bytes_written < 0) {
        perror("error writing page cache to disk:");
        exit(EXIT_FAILURE);
    }
}

void db_close(table_t* t)
{
    pager_t* pager;
    void* curr_page;
    u32 i, num_full_pages, num_additional_rows, page_num;
    int result;

    pager = t->pager;
    num_full_pages = t->row_count / ROWS_PER_PAGE;
    for (i = 0; i != num_full_pages; ++i) {
        curr_page = pager->pages[i];
        if (curr_page) {
            pager_flush(pager, i, PAGE_SIZE);
            xfree(curr_page);
        }
    }

    // if there's a partial write
    num_additional_rows = t->row_count % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        page_num = num_full_pages, curr_page = pager->pages[page_num];
        if (curr_page) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            xfree(curr_page);
        }
    }
    result = close(pager->fd);
    if (result < 0) {
        printf("error closing database.\n");
        exit(EXIT_FAILURE);
    }
    xfree(pager), xfree(t);
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
        db_close(t);
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

    st->row_to_insert.id = id, strcpy(st->row_to_insert.username, username),
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
    cursor_t* c = table_end(t);
    serialize_row(new_row, cursor_value(c));
    t->row_count++;
    xfree(c);

    return EXECUTE_SUCCESS;
}

execute_result_t exec_select(statement* st, table_t* t)
{
    row_t r;
    cursor_t* c;

    for (c = table_start(t); !(c->end_of_table); cursor_advance(c)) {
        deserialize_row(cursor_value(c), &r);
        print_row(&r);
    }
    xfree(c);

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

void close_input_buffer(input_buffer_t* in) { xfree(in->buf), xfree(in); }

void run_repl(const char* fname)
{
    table_t* table = db_open(fname);
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
            PRINT_TO_STDERR_NOARGS(
                "syntax error. could not parse statement.\n");
            continue;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("unrecognized keyword at start of '%s'.\n", user_input->buf);
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
    if (argc != 2) {
        printf("you must supply a database filename.\n");
        return EXIT_FAILURE;
    }
    const char* fname = argv[1];
    run_repl(fname);
    return EXIT_SUCCESS;
}
