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

// 将Excel文件中指定行的每个列的数据写入数据库键值表，值为下一行或下两行的数据，sort_order使用列号(从1开始)
func importKeysFromExcelToDB(db *sql.DB, countryCode, sku, excelPath string) error {
	f, err := excelize.OpenFile(excelPath)
	if err != nil {
		return fmt.Errorf("打开Excel文件失败: %v", err)
	}
	defer f.Close()

	// 定位到template工作表
	const sheetName = "template"
	index, _ := f.GetSheetIndex(sheetName)
	if index == -1 {
		return fmt.Errorf("工作表 %s 不存在", sheetName)
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return fmt.Errorf("读取工作表失败: %v", err)
	}

	// 确定数据起始行
	// 默认使用老模板（数据从第3行开始，索引为2）
	startRowIndex := 2

	// 判断是否为新模板
	isNewTemplate := false

	// 检查是否为新模板（第4行第一格的值是"SKU"）
	// 确保Excel文件至少有4行数据
	if len(rows) > 3 && len(rows[3]) > 0 && strings.TrimSpace(rows[3][0]) == "SKU" {
		// 新模板，数据从第5行开始（索引为4）
		startRowIndex = 4
		isNewTemplate = true
	}

	// 遍历该行的每个列
	for colIndex, cellValue := range rows[startRowIndex] {
		// 跳过空单元格
		if strings.TrimSpace(cellValue) == "" {
			continue
		}

		// 将每个列的数据作为spec_key插入数据库，列号作为sort_order
		specKey := cellValue
		sortOrder := colIndex + 1 // 排序从1开始

		// 根据模板类型确定spec_value的值
		var specValue string

		if isNewTemplate {
			// 新模板：spec_value为该列下两行的值（索引为startRowIndex+2）
			if len(rows) > startRowIndex+2 && len(rows[startRowIndex+2]) > colIndex {
				specValue = strings.TrimSpace(rows[startRowIndex+2][colIndex])
			} else {
				// 如果下两行不存在，则设置为空
				specValue = ""
			}
		} else {
			// 旧模板：spec_value为该列下一行的值（索引为startRowIndex+1）
			if len(rows) > startRowIndex+1 && len(rows[startRowIndex+1]) > colIndex {
				specValue = strings.TrimSpace(rows[startRowIndex+1][colIndex])
			} else {
				// 如果下一行不存在，则设置为空
				specValue = ""
			}
		}

		// 插入数据库
		_, err := db.Exec("INSERT INTO amz_pd_kv (sku_code, country_code, spec_key, spec_value, sort_order) VALUES (?,?,?,?,?)", sku, countryCode, specKey, specValue, sortOrder)
		if err != nil {
			return fmt.Errorf("插入数据失败: %v:行索引-%d:列索引-%d:值-%s", err, startRowIndex, colIndex, specKey)
		}
	}

	return nil
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

		// 判断是否为新模板
		isNewTemplate := false
		rows, _ := f.GetRows(sheetName)
		if len(rows) > 3 && len(rows[3]) > 0 && strings.TrimSpace(rows[3][0]) == "SKU" {
			isNewTemplate = true
		}

		// 根据模板类型确定列名所在的行索引
		colNameRowIndex := 2 // 默认旧模板，第三行（索引2）作为列名
		if isNewTemplate {
			colNameRowIndex = 4 // 新模板，第五行（索引4）作为列名
		}

		fmt.Printf("模板类型: %s, 列名所在行索引: %d\n", map[bool]string{true: "新模板", false: "旧模板"}[isNewTemplate], colNameRowIndex)

		// 创建列名映射
		columnMap := make(map[string]string)
		for idx, col := range cols {
			// 检查列是否有足够的行数
			if len(col) <= colNameRowIndex {
				continue // 跳过没有足够行数的列
			}
			colName := strings.TrimSpace(col[colNameRowIndex]) // 使用确定的行作为列名
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
		if len(os.Args) < 5 {
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

		// 使用修改后的importKeysFromExcelToDB函数导入数据
		if err := importKeysFromExcelToDB(db, countryCode, sku, filename); err != nil {
			fmt.Printf("导入数据失败: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("数据导入成功")

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
