/*
 * DMUtil.c
 *
 *  Created on: 2015-6-5
 *      Author: wh
 */
#include "DMUtil.h"
#include <DPI.h>
#include <DPIext.h>
#include <DPItypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * 功能：错误信息获取打印
 * hndl_type: 句柄类型
 * hndl:句柄
 * Return:无
 */
void dpi_err_msg_print(sdint2 hndl_type, dhandle hndl) {
	sdint4 err_code;
	sdint2 msg_len;
	sdbyte err_msg[SDBYTE_MAX];
	/* 获取错误信息集合 */
	dpi_get_diag_rec(hndl_type, hndl, 1, &err_code, err_msg, sizeof(err_msg),
			&msg_len);
	printf("err_msg = %s, err_code = %d\n", err_msg, err_code);
}

/* 函数检查及错误信息显示 */
#define DPIRETURN_CHECK(rt,hndl_type,hndl) if(!DSQL_SUCCEEDED(rt)){dpi_err_msg_print(hndl_type, hndl);return rt;}

/**
 * 功能：连接数据库
 * 输入：
 * server:服务器 IP
 * uid:数据库登录账号
 * pwd: 数据库登录密码
 * 输出：
 * henv：环境句柄
 * hcon：连接句柄
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_dpi_connect(sdbyte* server, sdbyte* uid, sdbyte* pwd, dhenv* henv,
		dhcon* hcon) {
	DPIRETURN rt;
	/* 申请环境句柄 */
	rt = dpi_alloc_env(henv);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_ENV, *henv);
	/* 申请连接句柄 */
	rt = dpi_alloc_con(*henv, hcon);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_DBC, *hcon);
	/* 连接数据库服务器 */
	rt = dpi_login(*hcon, server, uid, pwd);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_DBC, *hcon);
	printf("DM connect success!\n");
	return DSQL_SUCCESS;
}

/**
 * 功能： 断开数据库连接
 * 输入：
 * henv：环境句柄
 * hcon：连接句柄
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_dpi_disconnect(dhenv henv, dhcon hcon) {
	DPIRETURN rt;
	/* 断开连接 */
	rt = dpi_logout(hcon);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_DBC, hcon);
	/* 释放连接句柄和环境句柄 */
	rt = dpi_free_con(hcon);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_DBC, hcon);
	rt = dpi_free_env(henv);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_ENV, henv);
	printf("DM disconnect success!\n");
	return DSQL_SUCCESS;
}

/**
 * 功能：创建数据表
 * 输入：
 * hcon：连接句柄
 * sql：创建数据表的操作sql
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_create_table(dhcon hcon, sdbyte* sql) {
	DPIRETURN rt;
	dhstmt hstmt;
	/* 申请语句句柄 */
	rt = dpi_alloc_stmt(hcon, &hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 执行 sql */
	rt = dpi_exec_direct(hstmt, sql);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 释放语句句柄 */
	rt = dpi_free_stmt(hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	printf("DM table create success!\n");
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定int类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data:绑定的数据
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN int_bind_param(dhstmt* hstmt, int column_index, sdint4* data) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt = dpi_bind_param(*hstmt, column_index, DSQL_PARAM_INPUT, DSQL_C_SLONG,
			DSQL_INT, sizeof(*data), 0, data, sizeof(*data), (slength*) data);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, *hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定char类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data：绑定的数据
 * buf_len：buf长度
 * 输出：
 * data_ind_ptr：数据的实际长度，必须传出，dpi_exec要用到
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN char_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt = dpi_bind_param(hstmt, column_index, DSQL_PARAM_INPUT, DSQL_C_NCHAR,
			DSQL_CHAR, buf_len, 0, data, buf_len, data_ind_ptr);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定vchar类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data:绑定的数据
 * 输出：
 * data_ind_ptr：数据的实际长度，必须传出，dpi_exec要用到
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN vchar_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt = dpi_bind_param(hstmt, column_index, DSQL_PARAM_INPUT, DSQL_C_NCHAR,
			DSQL_VARCHAR, buf_len, 0, data, buf_len, data_ind_ptr);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定numeric类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data:绑定的数据
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN numeric_bind_param(dhstmt hstmt, int column_index, ddouble* data) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt
			= dpi_bind_param(hstmt, column_index, DSQL_PARAM_INPUT,
					DSQL_C_DOUBLE, DSQL_DOUBLE, sizeof(*data), 0, data,
					sizeof(*data), (slength*) data);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定timestamp类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data:绑定的数据
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN timestamp_bind_param(dhstmt hstmt, int column_index,
		dpi_timestamp_t *data) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt = dpi_bind_param(hstmt, column_index, DSQL_PARAM_INPUT,
			DSQL_C_TIMESTAMP, DSQL_TIMESTAMP, sizeof(*data), 0, data,
			sizeof(*data), (slength*) data);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定clob(字符大字段)类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data:绑定的数据
 * 输出：
 * data_ind_ptr：数据的实际长度，必须传出，dpi_exec要用到
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN clob_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt = dpi_bind_param(hstmt, column_index, DSQL_PARAM_INPUT, DSQL_C_NCHAR,
			DSQL_CLOB, buf_len, 0, data, buf_len, data_ind_ptr);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：绑定blob(二进制大字段)类型参数，语句句柄在函数外部释放
 * 输入：
 * hstmt：语句句柄
 * column_index：绑定对应的列索引
 * data:绑定的数据
 * 输出：
 * data_ind_ptr：数据的实际长度，必须传出，dpi_exec要用到
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN blob_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr) {
	DPIRETURN rt;
	/* 绑定参数 */
	rt = dpi_bind_param(hstmt, column_index, DSQL_PARAM_INPUT, DSQL_C_NCHAR,
			DSQL_BLOB, buf_len, 0, data, buf_len, data_ind_ptr);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：通过参数绑定的方式执行 sql 语句插入数据到数据库
 * 输入：
 * hcon：连接句柄
 * sql：插入数据库的操作sql
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_insert_with_bind_param(dhcon hcon, sdbyte* sql) {
	DPIRETURN rt;
	dhstmt hstmt;
	/* 分配语句句柄 */
	rt = dpi_alloc_stmt(hcon, &hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 准备 sql */
	rt = dpi_prepare(hstmt, sql);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);

	/*变量声明*/
	sdint4 c1;
	sdbyte c2[10];
	sdbyte c3[10];
	ddouble c4;
	dpi_timestamp_t c5;
	sdbyte c6[18];
	sdbyte c7[18];

	/* 字段变量赋值 */
	c1 = 1;
	memcpy(c2, "abcde", 5);
	memcpy(c3, "abcdefghi", 9);
	c4 = 1000.001;
	c5.year = 2011;
	c5.month = 3;
	c5.day = 1;
	c5.hour = 11;
	c5.minute = 45;
	c5.second = 50;
	c5.fraction = 900;
	memcpy(c6, "adfadsfetre2345ert", 18);
	memcpy(c7, "1234567890abcdef12", 18);

	/* 获取缓冲区长度 */
	slength c2_ind_ptr=5;
	slength c3_ind_ptr=9;
	slength c6_ind_ptr=18;
	slength c7_ind_ptr=18;

	/* 绑定参数 */
	if (int_bind_param(&hstmt, 1, &c1) != 0)
		return -1;
	if (char_bind_param(hstmt, 2, c2, sizeof(c2), &c2_ind_ptr) != 0)
		return -1;
	if (vchar_bind_param(hstmt, 3, c3, sizeof(c3), &c3_ind_ptr) != 0)
		return -1;
	if (numeric_bind_param(hstmt, 4, &c4) != 0)
		return -1;
	if (timestamp_bind_param(hstmt, 5, &c5) != 0)
		return -1;
	if (clob_bind_param(hstmt, 6, c6, sizeof(c6), &c6_ind_ptr) != 0)
		return -1;
	if (blob_bind_param(hstmt, 7, c7, sizeof(c7), &c7_ind_ptr) != 0)
		return -1;

	//	/* 绑定参数 */
	//	rt = dpi_bind_param(hstmt, 1, DSQL_PARAM_INPUT, DSQL_C_SLONG, DSQL_INT, sizeof(c1), 0,
	//	&c1, sizeof(c1), (slength*)&c1);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	//	rt = dpi_bind_param(hstmt, 2, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_CHAR, sizeof(c2),
	//	0, c2, sizeof(c2), &c2_ind_ptr);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	//	rt = dpi_bind_param(hstmt, 3, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_VARCHAR,
	//	sizeof(c3), 0, c3, sizeof(c3), &c3_ind_ptr);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	//	rt = dpi_bind_param(hstmt, 4, DSQL_PARAM_INPUT, DSQL_C_DOUBLE, DSQL_DOUBLE,
	//	sizeof(c4), 0, &c4, sizeof(c4), (slength*)&c4);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	//	rt=dpi_bind_param(hstmt,5,DSQL_PARAM_INPUT,DSQL_C_TIMESTAMP,DSQL_TIMESTAMP, sizeof(c5), 0,
	//	&c5, sizeof(c5), (slength*)&c5);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	//	rt = dpi_bind_param(hstmt, 6, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_CLOB, sizeof(c6),
	//	0, c6, sizeof(c6), &c6_ind_ptr);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	//	rt = dpi_bind_param(hstmt, 7, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_BLOB, sizeof(c7),
	//	0, c7, sizeof(c7), &c7_ind_ptr);
	//	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);

	/* 执行 sql */
	rt = dpi_exec(hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 释放语句句柄 */
	rt = dpi_free_stmt(hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	printf("dm insert with bind param success!\n");
	return DSQL_SUCCESS;
}

/**
 * 功能：通过参数绑定数组的方式执行 sql 语句
 * 输入：
 * hcon：连接句柄
 * rowNum：组数
 * sql：插入数据库的操作sql
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_insert_with_bind_array(dhcon hcon, int rowNum, sdbyte* sql) {
	DPIRETURN rt;
	dhstmt hstmt;
	/* 定义字段相应的变量 */
	sdint4 c1[rowNum];
	sdbyte c2[rowNum][10];
	sdbyte c3[rowNum][10];
	ddouble c4[rowNum];
	dpi_timestamp_t c5[rowNum];
	sdbyte c6[rowNum][18];
	sdbyte c7[rowNum][18];
	/* 缓冲区长度 */
	slength c2_ind_ptr[rowNum];
	slength c6_ind_ptr[rowNum];
	slength c3_ind_ptr[rowNum];
	slength c7_ind_ptr[rowNum];

	int i;
	int i_array_rows = rowNum;
	/* 分配语句句柄 */
	rt = dpi_alloc_stmt(hcon, &hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 设置语句句柄属性 */
	rt = dpi_set_stmt_attr(hstmt, DSQL_ATTR_PARAMSET_SIZE,
			(dpointer) i_array_rows, sizeof(i_array_rows));
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 准备 sql */
	rt = dpi_prepare(hstmt, sql);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 赋值 */
	for (i = 0; i < i_array_rows; i++) {
		c1[i] = i + 10;
		memcpy(c2[i], "abcde", 5);
		memcpy(c3[i], "abcdefghi", 9);
		c4[i] = 1000.001;
		c5[i].year = 2011;
		c5[i].month = 3;
		c5[i].day = 1;
		c5[i].hour = 11;
		c5[i].minute = 45;
		c5[i].second = 50;
		c5[i].fraction = 900;
		memcpy(c6[i], "adfadsfetre2345ert", 18);
		memcpy(c7[i], "1234567890abcdef12", 18);
		/* 获取缓冲区长度 */
		c2_ind_ptr[i] = 5;
		c3_ind_ptr[i] = 9;
		c6_ind_ptr[i] = 18;
		c7_ind_ptr[i] = 18;
	}
	/* 绑定参数 */
	rt = dpi_bind_param(hstmt, 1, DSQL_PARAM_INPUT, DSQL_C_SLONG, DSQL_INT,
			sizeof(c1[0]), 0, &c1[0], sizeof(c1[0]), (slength*) &c1[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	rt = dpi_bind_param(hstmt, 2, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_CHAR,
			sizeof(c2[0]), 0, c2[0], sizeof(c2[0]), &c2_ind_ptr[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	rt = dpi_bind_param(hstmt, 3, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_VARCHAR,
			sizeof(c3[0]), 0, c3[0], sizeof(c3[0]), &c3_ind_ptr[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	rt = dpi_bind_param(hstmt, 4, DSQL_PARAM_INPUT, DSQL_C_DOUBLE, DSQL_DOUBLE,
			sizeof(c4[0]), 0, &c4[0], sizeof(c4[0]), (slength*) &c4[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	rt = dpi_bind_param(hstmt, 5, DSQL_PARAM_INPUT, DSQL_C_TIMESTAMP,
			DSQL_TIMESTAMP, sizeof(c5[0]), 0, &c5[0], sizeof(c5[0]),
			(slength*) &c5[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	rt = dpi_bind_param(hstmt, 6, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_CLOB,
			sizeof(c6[0]), 0, c6[0], sizeof(c6[0]), &c6_ind_ptr[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	rt = dpi_bind_param(hstmt, 7, DSQL_PARAM_INPUT, DSQL_C_NCHAR, DSQL_BLOB,
			sizeof(c7[0]), 0, c7[0], sizeof(c7[0]), &c7_ind_ptr[0]);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 执行 sql */
	rt = dpi_exec(hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	/* 释放语句句柄 */
	rt = dpi_free_stmt(hstmt);
	DPIRETURN_CHECK(rt, DSQL_HANDLE_STMT, hstmt);
	printf("dm insert with bind array success\n");
	return DSQL_SUCCESS;
}

/************************************************
 fetch 获取结果集
 ************************************************/
/**
 * 功能：fetch 获取结果集
 * 输入：
 * hcon：连接句柄
 * sql：获取数据库的操作sql
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_select_with_fetch(dhcon hcon, sdbyte* sql) {
	dhstmt hstmt;
	/* 与字段匹配的变量,用于获取字段值 */
	sdint4 c1 = 0;
	sdbyte c2[20];
	sdbyte c3[50];
	ddouble c4;
	dpi_timestamp_t c5;
	sdbyte c6[50];
	sdbyte c7[500];
	/* 缓冲区 */
	slength c1_ind = 0;
	slength c2_ind = 0;
	slength c3_ind = 0;
	slength c4_ind = 0;
	slength c5_ind = 0;
	slength c6_ind = 0;
	slength c7_ind = 0;
	/* 行数 */
	ulength row_num;

	sdint4 dataflag = 0;
	/* 分配语句句柄 */
	DPIRETURN_CHECK(dpi_alloc_stmt(hcon, &hstmt), DSQL_HANDLE_STMT, hstmt);
	/* 执行 sql 语句 */
	DPIRETURN_CHECK(dpi_exec_direct(hstmt,sql),DSQL_HANDLE_STMT, hstmt);
	/* 绑定输出列 */
	DPIRETURN_CHECK(dpi_bind_col(hstmt,1,DSQL_C_SLONG,&c1,sizeof(c1),&c1_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 2, DSQL_C_NCHAR, &c2, sizeof(c2), &c2_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 3, DSQL_C_NCHAR, &c3, sizeof(c3), &c3_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 4, DSQL_C_DOUBLE, &c4, sizeof(c4), &c4_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 5, DSQL_C_TIMESTAMP, &c5, sizeof(c5), &c5_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 6, DSQL_C_NCHAR, &c6, sizeof(c6), &c6_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 7, DSQL_C_NCHAR, &c7, sizeof(c7), &c7_ind),
			DSQL_HANDLE_STMT, hstmt);
	printf("dm_select_with_fetch......\n");
	printf(
			"----------------------------------------------------------------------\n");
	while (dpi_fetch(hstmt, &row_num) != DSQL_NO_DATA) {
		printf("c1 = %d, c2 = %s, c3 = %s, c4 = %f, ", c1, c2, c3, c4);
		printf("c5= %d-%d-%d %d:%d:%d.%d\n", c5.year, c5.month, c5.day,
				c5.hour, c5.minute, c5.second, c5.fraction);
		printf("c6 = %s, c7 = %s\n", c6, c7);
		dataflag = 1;
	}
	printf(
			"----------------------------------------------------------------------\n");
	if (!dataflag) {
		printf("dm no data\n");
	}
	/* 释放语句句柄 */
	DPIRETURN_CHECK(dpi_free_stmt(hstmt), DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

/**
 * 功能：使用参数绑定后再 fetch 获取结果集
 * 输入：
 * hcon：连接句柄
 * sql：获取数据库的操作sql
 * Return:
 *  		DSQL_SUCCESS 执行成功
 *  		DSQL_ERROR 执行失败
 */
DPIRETURN dm_select_with_fetch_with_param(dhcon hcon, sdbyte* sql) {
	dhstmt hstmt;
	/* 与字段匹配的变量,用于获取字段值 */
	sdint4 c1 = 0;
	sdbyte c2[20];
	sdbyte c3[50];
	ddouble c4;
	dpi_timestamp_t c5;
	sdbyte c6[50];
	sdbyte c7[500];
	/* 缓冲区 */
	slength c1_ind = 0;
	slength c2_ind = 0;
	slength c3_ind = 0;
	slength c4_ind = 0;
	slength c5_ind = 0;
	slength c6_ind = 0;
	slength c7_ind = 0;
	/* 行数 */
	ulength row_num;
	sdint4 dataflag = 0;

	c1 = 10;//读取 c1=10 的数据******************************************条件

	/* 分配语句句柄 */
	DPIRETURN_CHECK(dpi_alloc_stmt(hcon, &hstmt), DSQL_HANDLE_STMT, hstmt);
	/* 准备 sql */
	DPIRETURN_CHECK(dpi_prepare(hstmt, (sdbyte*)"select c1,c2,c3,c4,c5,c6,c7 from dpi_demo where c1 = ?"),
			DSQL_HANDLE_STMT, hstmt);
	/* 绑定参数 */
	DPIRETURN_CHECK(dpi_bind_param(hstmt, 1, DSQL_PARAM_INPUT, DSQL_C_STINYINT,
					DSQL_INT, sizeof(c1), 0, &c1, sizeof(c1), (slength*)&c1), DSQL_HANDLE_STMT, hstmt);
	/* 执行 sql */
	DPIRETURN_CHECK(dpi_exec(hstmt), DSQL_HANDLE_STMT, hstmt);
	/* 绑定输出列 */
	DPIRETURN_CHECK(dpi_bind_col(hstmt,1,DSQL_C_SLONG,&c1,sizeof(c1),&c1_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 2, DSQL_C_NCHAR, &c2, sizeof(c2), &c2_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 3, DSQL_C_NCHAR, &c3, sizeof(c3), &c3_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 4, DSQL_C_DOUBLE, &c4, sizeof(c4), &c4_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 5, DSQL_C_TIMESTAMP, &c5, sizeof(c5), &c5_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 6, DSQL_C_NCHAR, &c6, sizeof(c6), &c6_ind),
			DSQL_HANDLE_STMT, hstmt);
	DPIRETURN_CHECK(dpi_bind_col(hstmt, 7, DSQL_C_NCHAR, &c7, sizeof(c7), &c7_ind),
			DSQL_HANDLE_STMT, hstmt);
	/* 打印输出信息 */
	printf("dm_select_with_fetch_with_param......\n");
	printf(
			"----------------------------------------------------------------------\n");
	while (dpi_fetch(hstmt, &row_num) != DSQL_NO_DATA) {
		printf("c1 = %d, c2 = %s, c3 = %s, c4 = %f, ", c1, c2, c3, c4);
		printf("c5= %d-%d-%d %d:%d:%d.%d\n", c5.year, c5.month, c5.day,
				c5.hour, c5.minute, c5.second, c5.fraction);
		printf("c6 = %s, c7 = %s\n", c6, c7);
		dataflag = 1;
	}
	printf(
			"----------------------------------------------------------------------\n");
	if (!dataflag) {
		printf("dm no data\n");
	}
	/* 释放语句句柄 */
	DPIRETURN_CHECK(dpi_free_stmt(hstmt), DSQL_HANDLE_STMT, hstmt);
	return DSQL_SUCCESS;
}

int main() {
	//	sdbyte* creatTableSql=(sdbyte*)"create table dpi_demo(c1 int, c2 char(20), c3 varchar(50), c4 numeric(7,3),c5 timestamp(5), c6 clob, c7 blob)";
	//		sdbyte* insertTableSql=(sdbyte*)"insert into dpi_demo(c1,c2,c3,c4,c5,c6,c7) values(?,?,?,?,?,?,?)";
	//	sdbyte* selectDataFromTableSql=(sdbyte*)"select c1,c2,c3,c4,c5,c6,c7 from dpi_demo";
	//	sdbyte* selectDataFromTableParamSql=(sdbyte*)"select c1,c2,c3,c4,c5,c6,c7 from dpi_demo where c1 = ?";

	sdbyte* insertTableSql=(sdbyte*)"insert into dpi_demo(c1,c2,c3,c4,c5,c6,c7) values(?,?,?,?,?,?,?)";

	dhenv henv;
	dhcon hcon;

	//连接数据库
	if (dm_dpi_connect((sdbyte*) DM_SVR, (sdbyte*) DM_USER, (sdbyte*) DM_PWD,
			&henv, &hcon) != 0)
		return -1;

	//创建数据库
	//	if(dm_create_table(hcon,creatTableSql)!=0) return -1;

	//通过参数绑定的方式插入数据
	if (dm_insert_with_bind_param(hcon, insertTableSql) != 0)
		return -1;
	//	if(dm_insert_with_bind_param(&hcon,insertTableSql,bind_param,NULL)!=0) return -1;

	//通过参数绑定数组的方式插入数据
	//	if(dm_insert_with_bind_array(hcon,5,insertTableSql)!=0) return -1;

	//	if(dm_select_with_fetch(&hcon,selectDataFromTableSql)!=0) return -1;
	//	if(dm_select_with_fetch_with_param(&hcon,selectDataFromTableParamSql)!=0) return -1;

	//断开数据库
	if (dm_dpi_disconnect(henv, hcon) != 0)
		return -1;

	return 0;
}
