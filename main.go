package invoiceupload

import (
	"context"
	"fmt"

	"awacs.com/invoice_function/models"
	"awacs.com/invoice_function/utils"

	"cloud.google.com/go/functions/metadata"
	"cloud.google.com/go/storage"
	"gorm.io/gorm"

	db "github.com/brkelkar/common_utils/databases"
	"github.com/brkelkar/common_utils/logger"

	"bufio"
	"io"
	"log"
	"strconv"
	"strings"

	"gorm.io/driver/sqlserver"
)

var (
	colMap        map[string]int
	colName       = []string{"USERID", "BILLNUMBER", "BILLDATE", "CHALLANNUMBER", "CHALLANDATE", "BUYERCODE", "REASON", "COMPANYNAME", "UPC", "PRODUCTCODE", "MRP", "BATCH", "EXPIRY", "QUANTITY", "FREEQUANTITY", "RATE", "AMOUNT", "DISCOUNT", "DISCOUNTAMOUNT", "ADDLSCHEME", "ADDLSCHEMEAMOUNT", "ADDLDISCOUNT", "ADDLDISCOUNTAMOUNT", "DEDUCTABLEBEFOREDISCOUNT", "MRPINCLUSIVETAX", "VATAPPLICATION", "VAT", "ADDLTAX", "CST", "SGST", "CGST", "IGST", "BASESCHEMEQUANTITY", "BASESCHEMEFREEQUANTITY", "NETINVOICEAMOUNT", "PAYMENTDUEDATE", "REMARKS", "SAVEDATE", "SYNDATE", "SYNCDATE", "PRODUCTNAME", "PRODUCTPACK", "EMONTH", "EXPMONTH", "CESS", "CESSAMOUNT", "SGSTAMOUNT", "CGSTAMOUNT", "IGSTAMOUNT", "TAXABLEAMOUNT", "HSN", "BARCODE", "ORDERNUMBER", "ORDERDATE", "LASTTRANSACTIONDATE"}
	dateFormatMap map[string]string
	err           error
	developerID   string
)

func init() {
	colMap = make(map[string]int)
	for _, val := range colName {
		colMap[val] = -1
	}

	// Creating a connection to the database
	dbConfig := db.BuildDBMsSQLConfig("35.200.178.187",
		1433,
		"sqlserver",
		"awacs_smart",
		"test",
	)
	db.DB = make(map[string]*gorm.DB)
	db.DB["awacs_smart"], err = gorm.Open(sqlserver.Open(db.DbMsSQLURL(dbConfig)), &gorm.Config{})
	db.DB["awacs_smart"].AutoMigrate()

	if err != nil {
		logger.Error("Error while connecting to db", err)
		log.Print(err)
	}

}

//InvoiceUpload cloud funtion to upload file
func InvoiceUpload(ctx context.Context, e models.GCSEvent) error {

	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("metadata.FromContext: %v", err)
	}
	log.Printf("Event ID: %v\n", meta.EventID)
	log.Printf("Event type: %v\n", meta.EventType)

	// Get storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Print(err)

	}

	filePath := e.Bucket + "/" + e.Name
	fileName := e.Name

	fileSplitSlice := strings.Split(fileName, "_")
	spiltLen := len(fileSplitSlice)

	// Check if file is in correct format or not
	if !(spiltLen == 7 || spiltLen == 6) {
		log.Print("Invalid file name")
	}
	if spiltLen == 6 {
		developerID = fileSplitSlice[5]
	}
	distributorShortCode := fileSplitSlice[1]

	replace := getReplaceStrings(distributorShortCode)
	//to Do
	lastModDate := e.Updated
	Currentdate := lastModDate.Format("2006-01-02")
	fileTableIndex := getQueryIndex(distributorShortCode, filePath, Currentdate)

	flag := 1
	var Invoice []models.Invoice

	// Get file reader
	rc, err := client.Bucket(e.Bucket).Object(e.Name).NewReader(ctx)
	if err != nil {
		log.Print(err)
		return err
	}

	reader := bufio.NewReader(rc)
	// Start reading file line by line
	for {
		line, error := reader.ReadString('\n')
		if error == io.EOF {
			break
		} else if error != nil {
			log.Print(error)
		}
		for _, replaceVal := range replace {
			// Replace values we get from table (Company name which has '|' as charator in name)
			line = strings.ReplaceAll(line, replaceVal.Search_String, replaceVal.Replace_String)
		}
		line = strings.TrimSpace(line)
		lineSlice := strings.Split(line, "|")
		var tempInvoice models.Invoice
		for i, val := range lineSlice {
			if flag == 1 {

				colMap[strings.ToUpper(val)] = i

			} else {

				switch i {
				case -1:
					break
				case colMap["BILLNUMBER"]:
					tempInvoice.BillNumber = val
				case colMap["BILLDATE"]:
					tempInvoice.BillDate, _ = utils.ConvertDate(val)
				case colMap["CHALLANNUMBER"]:
					tempInvoice.ChallanNumber = val
				case colMap["CHALLANDATE"]:
					tempInvoice.ChallanDate, _ = utils.ConvertDate(val)
				case colMap["BUYERCODE"]:
					tempInvoice.BuyerId = val
				case colMap["REASON"]:
					tempInvoice.Reason = val
				case colMap["UPC"]:
					tempInvoice.UPC = val
				case colMap["PRODUCTCODE"]:
					tempInvoice.SupplierProductCode = val
				case colMap["PRODUCTNAME"]:
					tempInvoice.SupplierProductName = val
				case colMap["PRODUCTPACK"]:
					tempInvoice.SupplierProductPack = val
				case colMap["MRP"]:
					tempInvoice.MRP, _ = strconv.ParseFloat(val, 64)
				case colMap["BATCH"]:
					tempInvoice.Batch = val
				case colMap["EXPIRY"]:
					tempInvoice.Expiry, _ = utils.ConvertDate(val)
				case colMap["QUANTITY"]:
					tempInvoice.Quantity, _ = strconv.ParseFloat(val, 64)
				case colMap["FREEQUANTITY"]:
					tempInvoice.FreeQuantity, _ = strconv.ParseFloat(val, 64)
				case colMap["RATE"]:
					tempInvoice.Rate, _ = strconv.ParseFloat(val, 64)
				case colMap["AMOUNT"]:
					tempInvoice.Amount, _ = strconv.ParseFloat(val, 64)
				case colMap["DISCOUNT"]:
					tempInvoice.Discount, _ = strconv.ParseFloat(val, 64)
				case colMap["DISCOUNTAMOUNT"]:
					tempInvoice.DiscountAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["ADDLSCHEME"]:
					tempInvoice.AddlScheme, _ = strconv.ParseFloat(val, 64)
				case colMap["ADDLSCHEMEAMOUNT"]:
					tempInvoice.AddlSchemeAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["ADDLDISCOUNT"]:
					tempInvoice.AddlDiscount, _ = strconv.ParseFloat(val, 64)
				case colMap["ADDLDISCOUNTAMOUNT"]:
					tempInvoice.AddlDiscountAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["DEDUCTABLEBEFOREDISCOUNT"]:
					tempInvoice.DeductableBeforeDiscount, _ = strconv.ParseFloat(val, 64)
				case colMap["MRPINCLUSIVETAX"]:
					tempInvoice.MRPInclusiveTax, _ = strconv.Atoi(val)
				case colMap["VATAPPLICATION"]:
					tempInvoice.VATApplication = val
				case colMap["VAT"]:
					tempInvoice.VAT, _ = strconv.ParseFloat(val, 64)
				case colMap["ADDLTAX"]:
					tempInvoice.AddlTax, _ = strconv.ParseFloat(val, 64)
				case colMap["CST"]:
					tempInvoice.CST, _ = strconv.ParseFloat(val, 64)
				case colMap["SGST"]:
					tempInvoice.SGST, _ = strconv.ParseFloat(val, 64)
				case colMap["CGST"]:
					tempInvoice.CGST, _ = strconv.ParseFloat(val, 64)
				case colMap["IGST"]:
					tempInvoice.IGST, _ = strconv.ParseFloat(val, 64)
				case colMap["BASESCHEMEQUANTITY"]:
					tempInvoice.BaseSchemeQuantity, _ = strconv.ParseFloat(val, 64)
				case colMap["BASESCHEMEFREEQUANTITY"]:
					tempInvoice.BaseSchemeFreeQuantity, _ = strconv.ParseFloat(val, 64)
				case colMap["PAYMENTDUEDATE"]:
					tempInvoice.PaymentDueDate, _ = utils.ConvertDate(val)
				case colMap["REMARKS"]:
					tempInvoice.Remarks = val
				case colMap["COMPANYNAME"]:
					tempInvoice.CompanyName = val
				case colMap["NETINVOICEAMOUNT"]:
					tempInvoice.NetInvoiceAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["LASTTRANSACTIONDATE"]:
					tempInvoice.LastTransactionDate, _ = utils.ConvertDate(val)
				case colMap["SGSTAMOUNT"]:
					tempInvoice.SGSTAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["CGSTAMOUNT"]:
					tempInvoice.CGSTAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["IGSTAMOUNT"]:
					tempInvoice.IGSTAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["CESS"]:
					tempInvoice.Cess, _ = strconv.ParseFloat(val, 64)
				case colMap["CESSAMOUNT"]:
					tempInvoice.CessAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["TAXABLEAMOUNT"]:
					tempInvoice.TaxableAmount, _ = strconv.ParseFloat(val, 64)
				case colMap["HSN"]:
					tempInvoice.HSN = val
				case colMap["ORDERNUMBER"]:
					tempInvoice.OrderNumber = val
				case colMap["ORDERDATE"]:
					tempInvoice.OrderDate, _ = utils.ConvertDate(val)
				case colMap["BARCODE"]:
					tempInvoice.Barcode = val

				}

			}

		}
		if flag == 0 {
			tempInvoice.DeveloperId = developerID
			tempInvoice.File_Path = filePath
			tempInvoice.File_Received_Dttm, _ = utils.ConvertDate(Currentdate)
			tempInvoice.SupplierId = distributorShortCode
			tempInvoice.Inv_File_Id = fileTableIndex
			Invoice = append(Invoice, tempInvoice)
		}
		flag = 0
	}
	recordCount := len(Invoice)
	if recordCount > 0 {

		db.DB["awacs_smart"].AutoMigrate(&models.Invoice{})
		//Insert records to temp table
		db.DB["awacs_smart"].Save(Invoice)
	}
	updateFileDetails(fileTableIndex, Invoice[0].TableName(), recordCount)
	return nil
}

func getReplaceStrings(distributorCode string) []models.ReplaceStrings {
	var replace []models.ReplaceStrings
	db.DB["awacs_smart"].Where("Supplierid =?", distributorCode).First(&replace)
	return replace
}

func getQueryIndex(distributorShortCode string, filePath string, CurrentDate string) float64 {
	var queryObj models.FileIndetityQuery
	data := map[string]interface{}{
		"DistributorID": distributorShortCode,
		"FilePath":      filePath,
		"CurrentDate":   CurrentDate,
	}
	query, err := queryObj.GetFileIndexQuery(data)
	if err != nil {
		log.Print("Failed to update file details")
	}

	var fileTableIndex float64
	db.DB["awacs_smart"].Raw(query).Scan(&fileTableIndex)
	return fileTableIndex
}

func updateFileDetails(fileTableIndex float64, tableName string, recordCount int) {
	var queryObj models.FileIndetityQuery

	data := map[string]interface{}{
		"FileID":      fileTableIndex,
		"TableName":   tableName,
		"RecordCount": recordCount,
	}

	query, err := queryObj.GetUpdateFileIndexQuery(data)

	if err != nil {
		log.Print("Failed to update file details")
	}
	db.DB["awacs_smart"].Exec(query)

}
