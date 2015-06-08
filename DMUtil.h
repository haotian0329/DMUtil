/*
 * DMUtil.h
 *
 *  Created on: 2015-6-5
 *      Author: wh
 */

#ifndef DMUTIL_H_
#define DMUTIL_H_

#include <DPI.h>

//数据库登录信息
#define DM_SVR "LOCALHOST"
#define DM_USER "SYSDBA"
#define DM_PWD "123456"

//连接数据库
DPIRETURN dm_dpi_connect(sdbyte* server, sdbyte* uid, sdbyte* pwd, dhenv* henv,
		dhcon* hcon);

//断开数据库
DPIRETURN dm_dpi_disconnect(dhenv henv, dhcon hcon);

//创建数据表
DPIRETURN dm_create_table(dhcon hcon, sdbyte* sql);

//绑定int类型参数
DPIRETURN int_bind_param(dhstmt* hstmt, int column_index, sdint4* data);
//绑定char类型参数
DPIRETURN char_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr);
//绑定vchar类型参数
DPIRETURN vchar_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr);
//绑定numeric类型参数
DPIRETURN numeric_bind_param(dhstmt hstmt, int column_index, ddouble* data);
//绑定timestamp类型参数
DPIRETURN timestamp_bind_param(dhstmt hstmt, int column_index,
		dpi_timestamp_t* data);
//绑定clob(字符大字段)类型参数
DPIRETURN clob_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr);
//绑定blob(二进制大字段)类型参数
DPIRETURN blob_bind_param(dhstmt hstmt, int column_index, sdbyte* data,
		int buf_len, slength* data_ind_ptr);

//通过参数绑定的方式执行 sql 语句插入数据到数据库
DPIRETURN dm_insert_with_bind_param(dhcon hcon, sdbyte* sql);

//通过参数绑定数组的方式执行 sql 语句插入数据到数据库
DPIRETURN dm_insert_with_bind_array(dhcon hcon, int rowNum, sdbyte* sql);

//fetch 获取结果集
DPIRETURN dm_select_with_fetch(dhcon hcon, sdbyte* sql);

//使用参数绑定后再 fetch 获取结果集
DPIRETURN dm_select_with_fetch_with_param(dhcon hcon, sdbyte* sql);

#endif /* DMUTIL_H_ */
