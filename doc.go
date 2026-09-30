// Package columnstore 实现纯本地的列式记录存储：
// 按批写入、按列读回，支持 schema 演进与后台合并压缩。
package columnstore
