#include "bencode_util.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Encode the start of a bencode dictionary */
int bencode_encode_dict_start(char *buf, size_t *offset, size_t buf_size) {
    if (!buf || !offset || *offset >= buf_size) {
        return -1;
    }

    buf[(*offset)++] = 'd';
    return 0;
}

/* Encode the end of a bencode dictionary */
int bencode_encode_dict_end(char *buf, size_t *offset, size_t buf_size) {
    if (!buf || !offset || *offset >= buf_size) {
        return -1;
    }

    buf[(*offset)++] = 'e';
    return 0;
}

/* Encode a bencode string */
int bencode_encode_string(char *buf, size_t *offset, size_t buf_size,
                         const char *str, size_t str_len) {
    if (!buf || !offset || !str) {
        return -1;
    }

    /* Calculate required space: length digits + ':' + string */
    char len_str[32];
    int len_digits = snprintf(len_str, sizeof(len_str), "%zu", str_len);

    if (*offset + len_digits + 1 + str_len > buf_size) {
        return -1;
    }

    /* Write length */
    memcpy(buf + *offset, len_str, len_digits);
    *offset += len_digits;

    /* Write colon */
    buf[(*offset)++] = ':';

    /* Write string data */
    memcpy(buf + *offset, str, str_len);
    *offset += str_len;

    return 0;
}

/* Encode a bencode integer */
int bencode_encode_int(char *buf, size_t *offset, size_t buf_size, int64_t val) {
    if (!buf || !offset) {
        return -1;
    }

    char int_str[64];
    int len = snprintf(int_str, sizeof(int_str), "i%llde", (long long)val);

    if (*offset + len > buf_size) {
        return -1;
    }

    memcpy(buf + *offset, int_str, len);
    *offset += len;

    return 0;
}

/* Encode a key-value pair */
int bencode_encode_key_value(char *buf, size_t *offset, size_t buf_size,
                             const char *key, const char *value, size_t value_len) {
    if (bencode_encode_string(buf, offset, buf_size, key, strlen(key)) != 0) {
        return -1;
    }

    if (bencode_encode_string(buf, offset, buf_size, value, value_len) != 0) {
        return -1;
    }

    return 0;
}

/* Simple bencode parser for DHT messages */
/* This is a simplified parser - for production use, integrate with bencode-c library */

/* Find a value in a bencoded dictionary by key */
static const char* find_dict_value(const char *dict, size_t dict_len,
                                  const char *key, size_t *value_len, char *value_type) {
    const char *p = dict;
    const char *end = dict + dict_len;

    if (!dict || dict_len == 0 || *p != 'd') {
        return NULL;
    }

    p++; /* skip 'd' */

    while (p < end && *p != 'e') {
        /* Parse key (must be a string) */
        if (*p < '0' || *p > '9') {
            return NULL;
        }

        /* Get key length */
        size_t key_len = 0;
        while (p < end && *p >= '0' && *p <= '9') {
            key_len = key_len * 10 + (*p - '0');
            p++;
        }

        if (p >= end || *p != ':') {
            return NULL;
        }
        p++; /* skip ':' */

        const char *key_str = p;
        p += key_len;

        if (p >= end) {
            return NULL;
        }

        /* Check if this is the key we're looking for */
        int key_match = (strlen(key) == key_len && memcmp(key_str, key, key_len) == 0);

        /* Parse value */
        const char *value_start = p;

        if (*p == 'i') {
            /* Integer value */
            p++;
            while (p < end && *p != 'e') p++;
            if (p < end) p++; /* skip 'e' */

            if (key_match) {
                if (value_type) *value_type = 'i';
                if (value_len) *value_len = p - value_start;
                return value_start;
            }
        } else if (*p >= '0' && *p <= '9') {
            /* String value */
            size_t str_len = 0;
            while (p < end && *p >= '0' && *p <= '9') {
                str_len = str_len * 10 + (*p - '0');
                p++;
            }
            if (p < end && *p == ':') {
                p++;
                p += str_len;

                if (key_match) {
                    if (value_type) *value_type = 's';
                    if (value_len) *value_len = p - value_start;
                    return value_start;
                }
            }
        } else if (*p == 'd' || *p == 'l') {
            /* Dictionary or list - skip it */
            int depth = 1;
            char open = *p;
            p++;

            while (p < end && depth > 0) {
                if (*p == 'd' || *p == 'l') {
                    depth++;
                } else if (*p == 'e') {
                    depth--;
                } else if (*p >= '0' && *p <= '9') {
                    /* Skip string */
                    size_t str_len = 0;
                    while (p < end && *p >= '0' && *p <= '9') {
                        str_len = str_len * 10 + (*p - '0');
                        p++;
                    }
                    if (p < end && *p == ':') {
                        p++;
                        p += str_len;
                        continue;
                    }
                } else if (*p == 'i') {
                    /* Skip integer */
                    p++;
                    while (p < end && *p != 'e') p++;
                }
                p++;
            }

            if (key_match) {
                if (value_type) *value_type = open;
                if (value_len) *value_len = p - value_start;
                return value_start;
            }
        }
    }

    return NULL;
}

/* Parse a DHT message to extract message type and transaction ID */
int bencode_parse_message(const char *data, size_t len,
                         char *msg_type, size_t msg_type_size,
                         char *tid, size_t *tid_len) {
    if (!data || len == 0) {
        return -1;
    }

    /* Get message type ('q' = query, 'r' = response, 'e' = error) */
    size_t value_len;
    char value_type;
    const char *y_value = find_dict_value(data, len, "y", &value_len, &value_type);

    if (y_value && value_type == 's' && value_len >= 3) {
        /* Parse string length and data */
        const char *p = y_value;
        size_t str_len = *p - '0';
        p += 2; /* skip length and ':' */

        if (str_len > 0 && str_len < msg_type_size) {
            memcpy(msg_type, p, str_len);
            msg_type[str_len] = '\0';
        }
    }

    /* Get transaction ID */
    const char *t_value = find_dict_value(data, len, "t", &value_len, &value_type);

    if (t_value && value_type == 's' && tid && tid_len) {
        const char *p = t_value;
        size_t str_len = 0;

        /* Parse string length */
        while (*p >= '0' && *p <= '9') {
            str_len = str_len * 10 + (*p - '0');
            p++;
        }

        if (*p == ':') {
            p++;
            if (str_len <= *tid_len) {
                memcpy(tid, p, str_len);
                *tid_len = str_len;
            }
        }
    }

    return 0;
}

/* Get a dictionary value */
int bencode_get_dict_value(const char *dict, size_t dict_len,
                           const char *key, bencode_value_t *value) {
    if (!dict || !key || !value) {
        return -1;
    }

    size_t value_len;
    char value_type;
    const char *val = find_dict_value(dict, dict_len, key, &value_len, &value_type);

    if (!val) {
        return -1;
    }

    if (value_type == 's') {
        value->type = BENCODE_TYPE_STRING;
        /* Parse string */
        const char *p = val;
        size_t str_len = 0;

        while (*p >= '0' && *p <= '9') {
            str_len = str_len * 10 + (*p - '0');
            p++;
        }

        if (*p == ':') {
            p++;
            value->value.str_val.data = p;
            value->value.str_val.len = str_len;
        }
    } else if (value_type == 'i') {
        value->type = BENCODE_TYPE_INT;
        /* Parse integer */
        const char *p = val + 1; /* skip 'i' */
        int64_t int_val = 0;
        int negative = 0;

        if (*p == '-') {
            negative = 1;
            p++;
        }

        while (*p >= '0' && *p <= '9') {
            int_val = int_val * 10 + (*p - '0');
            p++;
        }

        value->value.int_val = negative ? -int_val : int_val;
    }

    return 0;
}
