package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL驱动
	"github.com/xuri/excelize/v2"
)

// 数据库配置（根据实际修改）
const (
	dbUser     = "jia"
	dbPassword = "123"
	dbHost     = "192.168.30.6"
	dbPort     = "3306"
	dbName     = "jdb"
)

// 从数据库获取键值对数据
func fetchKeyValuesFromDB(db *sql.DB, countryCode, sku string) (map[string]string, error) {
	query := `
		SELECT spec_key, spec_value 
		FROM amz_pd_kv 
		WHERE country_code = ? AND sku_code = ?
	`
	rows, err := db.Query(query, countryCode, sku)
	if err != nil {
		return nil, fmt.Errorf("数据库查询失败: %v", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("数据解析失败: %v", err)
		}
		result[key] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("结果遍历失败: %v", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("未找到匹配的数据")
	}

	return result, nil
}

func main() {
	// (1) 处理命令行参数
	if len(os.Args) < 4 {
		fmt.Println("使用方法: program <excel文件名> <国家代码> <SKU>")
		os.Exit(1)
	}
	filename := os.Args[1]
	countryCode := os.Args[2]
	sku := os.Args[3]

	// 连接数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True",
		dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("数据库连接失败: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 测试数据库连接
	if err := db.Ping(); err != nil {
		fmt.Printf("数据库连接测试失败: %v\n", err)
		os.Exit(1)
	}

	// (2) 从数据库获取数据
	data, err := fetchKeyValuesFromDB(db, countryCode, sku)
	if err != nil {
		fmt.Printf("获取数据失败: %v\n", err)
		os.Exit(1)
	}

	// 打开Excel文件
	f, err := excelize.OpenFile(filename)
	if err != nil {
		fmt.Printf("打开Excel文件失败: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("关闭文件失败: %v\n", err)
		}
	}()

	// 定位到template工作表
	const sheetName = "template"
	index, _ := f.GetSheetIndex(sheetName)
	if index == -1 {
		fmt.Printf("工作表 %s 不存在\n", sheetName)
		os.Exit(1)
	}

	// 查找第一个空行（从第2行开始检查）
	emptyRow := 0
	for row := 2; ; row++ {
		cellValue, _ := f.GetCellValue(sheetName, fmt.Sprintf("A%d", row))
		if cellValue == "" {
			emptyRow = row
			break
		}
	}
	if emptyRow == 0 {
		fmt.Println("未找到可用空行")
		os.Exit(1)
	}

	// 获取列映射
	cols, err := f.GetCols(sheetName)
	if err != nil {
		fmt.Printf("获取列失败: %v\n", err)
		os.Exit(1)
	}

	// 创建列名映射（第三行作为列名）
	columnMap := make(map[string]string)
	for idx, col := range cols {
		colName := strings.TrimSpace(col[2])
		fmt.Printf("Col Name: %v\n", colName)
		if colName != "" {
			columnName, _ := excelize.ColumnNumberToName(idx + 1)
			columnMap[colName] = columnName
		}
	}

	// 写入数据到对应列
	writeCount := 0
	for key, value := range data {
		col, exists := columnMap[key]
		if !exists {
			fmt.Printf("警告: 列 %s 不存在\n", key)
			continue
		}

		cell := fmt.Sprintf("%s%d", col, emptyRow)
		if err := f.SetCellValue(sheetName, cell, value); err != nil {
			fmt.Printf("写入单元格 %s 失败: %v\n", cell, err)
			continue
		}
		writeCount++
	}

	if writeCount == 0 {
		fmt.Println("没有数据被写入，请检查列名匹配")
		os.Exit(1)
	}

	// 保存为新文件
	newFilename := "go11.xlsx"
	if err := f.SaveAs(newFilename); err != nil {
		fmt.Printf("保存文件失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("成功写入 %d 条数据，文件已保存为: %s\n", writeCount, newFilename)
}
