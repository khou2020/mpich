#include <string.h>
#include <assert.h>
#include "adio.h"
#include "adioi.h"
#include "ad_logfs_common.h"

int ad_logfs_checkbool(const char *buf)
{
    if (!buf || !buf[0])
        return 0;
    ADIOI_Strlower((char *) buf);

    if (!strcmp(buf, "1"))
        return 1;
    if (!strcmp(buf, "true"))
        return 1;

    return 0;
}


int ad_logfs_hint_bool(MPI_Info info, const char *key, int *val)
{
    int flag;
    char buf[255];

    if (info == MPI_INFO_NULL)
        return 0;

    MPI_Info_get(info, (char *) key, sizeof(buf) - 1, &buf[0], &flag);
    if (!flag)
        return 0;

    *val = ad_logfs_checkbool(buf);
    return 1;
}

int ad_logfs_hint_int(MPI_Info info, const char *key, int *val)
{
    int flag;
    char buf[255];

    if (info == MPI_INFO_NULL)
        return 0;

    MPI_Info_get(info, (char *) key, sizeof(buf) - 1, &buf[0], &flag);
    if (!flag)
        return 0;

    *val = atoi(buf);
    return 1;
}

int ad_logfs_hint_str(MPI_Info info, const char *key, char **str)
{
    int flag;
    char buf[255];

    if (info == MPI_INFO_NULL)
        return 0;

    MPI_Info_get(info, (char *) key, sizeof(buf) - 1, &buf[0], &flag);
    if (!flag)
        return 0;

    if (*str)
        ADIOI_Free(*str);

    *str = ADIOI_Strdup(buf);

    return 1;
}

void ad_logfs_hint_set_bool(MPI_Info info, const char *key, int val)
{
    assert(info != MPI_INFO_NULL);
    MPI_Info_set(info, (char *) key, (val ? "true" : "false"));
}

void ad_logfs_hint_set_int(MPI_Info info, const char *key, int val)
{
    assert(info != MPI_INFO_NULL);
    char buf[255];
    snprintf(buf, sizeof(buf) - 1, "%i", val);
    MPI_Info_set(info, (char *) key, buf);
}

/* if 'str' is NULL, info won't be set and key will not exist in info object */
void ad_logfs_hint_set_str(MPI_Info info, const char *key, const char *str)
{
    assert(info != MPI_INFO_NULL);
    if (str != NULL)
        MPI_Info_set(info, (char *) key, (char *) str);
}
