package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xuri/excelize/v2"
)

// 数据库配置（根据实际修改）
const (
	dbUser     = "jia"
	dbPassword = "123"
	dbPort     = "3306"
	dbName     = "jdb"
)

var dbHost = os.Getenv("DB_HOST")

// 从数据库获取键值对数据
func fetchKeyValuesFromDB(db *sql.DB, countryCode, sku string) (map[string]string, error) {
	query := `
		SELECT spec_key, spec_value 
		FROM amz_pd_kv 
		WHERE country_code =? AND sku_code =?
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
			//return nil, fmt.Errorf("数据解析失败: %v", err)
			value = "" // 如果读取失败，设置值为NULL
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

// 将现有数据库中指定的国家和sku的键值数据复制到另外一个sku上
func copyKeyValuesInDB(db *sql.DB, countryCode, sku, newCode, newSku string) error {
	query := `
		SELECT spec_key, spec_value, sort_order
		FROM amz_pd_kv 
		WHERE country_code =? AND sku_code =?
	`
	rows, err := db.Query(query, countryCode, sku)
	if err != nil {
		return fmt.Errorf("数据库查询失败: %v", err)
	}
	defer rows.Close()

	// 准备插入语句
	insertStmt, err := db.Prepare("INSERT IGNORE INTO amz_pd_kv (sku_code, country_code, spec_key, spec_value, sort_order) VALUES (?,?,?,?,?)")
	if err != nil {
		return fmt.Errorf("准备插入语句失败: %v", err)
	}
	defer insertStmt.Close()

	// 遍历查询结果并插入到新的SKU
	for rows.Next() {
		var key, value string
		var order int
		if err := rows.Scan(&key, &value, &order); err != nil {
			value = "" // 如果读取失败，设置值为NULL
			order = 0  // 如果读取失败，设置排序为0
			fmt.Printf("数据读取错误: %v", err)
		}

		//fmt.Printf("数据读取:%s,%d\n", value, order)

		_, err := insertStmt.Exec(newSku, newCode, key, value, order)
		if err != nil {
			return fmt.Errorf("插入新SKU数据失败: %v", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("结果遍历失败: %v", err)
	}

	return nil
}

// 删除数据库中指定国家和SKU的键值数据
func deleteKeyValuesFromDB(db *sql.DB, countryCode, sku string) error {
	query := `
		DELETE FROM amz_pd_kv 
		WHERE country_code =? AND sku_code =?
	`
	_, err := db.Exec(query, countryCode, sku)
	if err != nil {
		return fmt.Errorf("删除数据失败: %v", err)
	}
	return nil
}

func main() {
	// (1) 处理命令行参数
	if len(os.Args) < 2 {
		fmt.Println("使用方法: amzfile <write/import/copy/delete> [Excel File] [Country Code] [SKU]...")
		os.Exit(1)
	}
	command := os.Args[1]

	switch command {
	case "write":
		if len(os.Args) < 5 {
			fmt.Println("使用方法: amz-file write <excel文件名> <国家代码> <SKU>")
			os.Exit(1)
		}
		filename := os.Args[2]
		countryCode := os.Args[3]
		sku := os.Args[4]

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
		emptyRow := 2
		for {
			cellValue, _ := f.GetCellValue(sheetName, fmt.Sprintf("A%d", emptyRow))
			if cellValue == "" {
				break
			}
			emptyRow++
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
			colName := strings.TrimSpace(col[4])
			fmt.Printf("Col Name: %v\n", colName)
			if colName != "" {
				columnName, _ := excelize.ColumnNumberToName(idx + 1)
				columnMap[colName] = columnName
			}
		}

		// 获取数据库数据
		writeCount := 0
		data, err := fetchKeyValuesFromDB(db, countryCode, sku)
		if err != nil {
			fmt.Printf("获取数据失败: %v\n", err)
			os.Exit(1)
		}

		//os.Exit(1)

		// 在main函数的write命令分支中修改数据写入循环
		for key, value := range data {
			col, exists := columnMap[key]
			if !exists {
				fmt.Printf("警告: 列 %s 不存在\n", key)
				continue
			}

			// 新增检查：如果value为空字符串，则跳过写入
			if value == "" || value == "null" {
				fmt.Printf("跳过空值: %s\n", key)
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
		//newFilename := "output.xlsx"
		if err := f.Save(); err != nil {
			fmt.Printf("保存文件失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("成功写入 %d 条数据，文件已保存!\n", writeCount)

	case "import":
		if len(os.Args) < 4 {
			fmt.Println("使用方法: amz-file import <excel文件名> <国家代码> <SKU>")
			os.Exit(1)
		}
		filename := os.Args[2]
		countryCode := os.Args[3]
		sku := os.Args[4]

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

		// 读取工作表的所有行
		rows, err := f.GetRows(sheetName)
		if err != nil {
			fmt.Printf("读取工作表失败: %v\n", err)
			os.Exit(1)
		}

		// 遍历行和列读取单元格数据并写入数据库
		fmt.Printf("读取 %d 行模板数据\n", len(rows)-1)
		fmt.Printf("sku_code, country_code: %s,%s \n", sku, countryCode)
		//fmt.Printf("rows[4]: %s \n", rows[3])

		if len(rows) < 4 {
			fmt.Println("Excel文件中没有数据")
			os.Exit(1)
		}

		for sidx, key := range rows[4] {
			specKey := key
			//_, err := db.Exec("INSERT INTO amz_pd_kv (sku_code, country_code, spec_key, spec_value) VALUES (?,?,?,NULL)", sku, countryCode, specKey)
			_, err := db.Query("INSERT IGNORE INTO amz_pd_kv (sku_code, country_code, spec_key, spec_value) VALUES (?,?,?,NULL)", sku, countryCode, specKey)
			if err != nil {
				fmt.Printf("插入数据失败: %v:%d\n", err, sidx)
				os.Exit(1)
			}
		}

	case "copy":
		if len(os.Args) < 6 {
			fmt.Println("使用方法: amz-file copy <源国家代码> <源SKU> <国家代码> <SKU>")
			os.Exit(1)
		}
		countryCode := os.Args[2]
		sku := os.Args[3]
		newCode := os.Args[4]
		newSku := os.Args[5]

		// 连接数据库
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True",
			dbUser, dbPassword, dbHost, dbPort, dbName)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			fmt.Printf("数据库连接失败: %v\n", err)
			os.Exit(1)
		}
		defer db.Close()

		// 执行复制操作
		if err := copyKeyValuesInDB(db, countryCode, sku, newCode, newSku); err != nil {
			fmt.Printf("复制数据失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("数据复制成功")

	case "delete":
		if len(os.Args) < 4 {
			fmt.Println("使用方法: program delete <国家代码> <SKU>")
			os.Exit(1)
		}
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

		// 执行删除操作
		if err := deleteKeyValuesFromDB(db, countryCode, sku); err != nil {
			fmt.Printf("删除数据失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("数据库数据删除成功")

	default:
		fmt.Println("未知命令，请使用写入、导入或复制命令")
	}
}
