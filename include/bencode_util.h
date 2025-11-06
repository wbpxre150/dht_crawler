#ifndef BENCODE_UTIL_H
#define BENCODE_UTIL_H

#include <stdint.h>
#include <stddef.h>

/* Bencode data types */
typedef enum {
    BENCODE_TYPE_INT,
    BENCODE_TYPE_STRING,
    BENCODE_TYPE_LIST,
    BENCODE_TYPE_DICT
} bencode_type_t;

/* Bencode value structure */
typedef struct bencode_value {
    bencode_type_t type;
    union {
        int64_t int_val;
        struct {
            const char *data;
            size_t len;
        } str_val;
    } value;
} bencode_value_t;

/* Function declarations */
int bencode_encode_dict_start(char *buf, size_t *offset, size_t buf_size);
int bencode_encode_dict_end(char *buf, size_t *offset, size_t buf_size);
int bencode_encode_string(char *buf, size_t *offset, size_t buf_size,
                          const char *str, size_t str_len);
int bencode_encode_int(char *buf, size_t *offset, size_t buf_size, int64_t val);
int bencode_encode_key_value(char *buf, size_t *offset, size_t buf_size,
                             const char *key, const char *value, size_t value_len);

/* Parsing functions */
int bencode_parse_message(const char *data, size_t len,
                         char *msg_type, size_t msg_type_size,
                         char *tid, size_t *tid_len);
int bencode_get_dict_value(const char *dict, size_t dict_len,
                           const char *key, bencode_value_t *value);

#endif /* BENCODE_UTIL_H */
