package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	pq "github.com/lib/pq"
)

func main() {
	s := gin.Default()

	var err error
	var db *sql.DB

	if os.Getenv("POSTGRES_USER") != "" {
		connInfo := fmt.Sprintf(
			"user=%s dbname=%s password=%s host=%s port=%s sslmode=disable",
			os.Getenv("POSTGRES_USER"),
			os.Getenv("POSTGRES_DATABASE"),
			os.Getenv("POSTGRES_PASSWORD"),
			os.Getenv("POSTGRES_PORT_5432_TCP_ADDR"),
			os.Getenv("POSTGRES_PORT_5432_TCP_PORT"),
		)
		db, err = sql.Open("postgres", connInfo)
	} else {
		db, err = sql.Open("postgres", "user=testservice dbname=test_service sslmode=disable")
	}

	if err != nil {
		fmt.Printf("Couldn't connect to postgres: %v", err)
	}
	defer db.Close()

	conditionalCreateTable(db)

	sampleCodes := getSampleCodes()
	loadCodes(sampleCodes, db)

	s.GET("/codes", func(c *gin.Context) {
		queryString := c.Query("query")
		resultCodes := queryCodes(queryString, db)
		c.JSON(200, resultCodes)
	})

	s.Run(":8080")
}

func conditionalCreateTable(db *sql.DB) {
	tables, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_name = 'codes'")
	defer tables.Close()
	var tableResults []string
	for tables.Next() {
		var table string
		err = tables.Scan(&table)
		if err != nil {
			fmt.Printf("Error scanning table rows: %v", err)
		}
		tableResults = append(tableResults, table)
	}
	if len(tableResults) == 1 {
		fmt.Println("Codes table found. Skipping creation.")
	} else if len(tableResults) == 0 {
		fmt.Println("Codes table not found. Creating table.")

		txn, err := db.Begin()
		if err != nil {
			fmt.Printf("Couldn't create transaction: %v", err)
		}

		stmt, err := txn.Prepare("CREATE TABLE codes (code text, description text)")
		_, err = stmt.Exec()
		if err != nil {
			fmt.Printf("Couldn't execute table creation transaction statement: %v", err)
		}

		err = stmt.Close()
		if err != nil {
			fmt.Printf("Couldn't close table creation transaction statement: %v", err)
		}

		err = txn.Commit()
		if err != nil {
			fmt.Printf("Couldn't commit table creation transaction: %v", err)
		}

	}
}

func queryCodes(queryString string, db *sql.DB) [][]string {
	rows, err := db.Query("SELECT * FROM codes WHERE code LIKE '%' || $1 || '%' OR description LIKE '%' || $1 || '%'", queryString)
	if err != nil {
		fmt.Printf("Error querying postgres: %v", err)
	}
	var results [][]string
	defer rows.Close()
	for rows.Next() {
		var code string
		var desc string
		err = rows.Scan(&code, &desc)
		if err != nil {
			fmt.Printf("Error scanning result rows: %v", err)
		}
		results = append(results, []string{code, desc})
	}
	err = rows.Err()
	if err != nil {
		fmt.Printf("Error during result iteration: %v", err)
	}
	return results
}

func loadCodes(sampleCodes [][]string, db *sql.DB) {
	rows, err := db.Query("SELECT count(1) FROM codes")
	if err != nil {
		fmt.Printf("Error querying postgres: %v", err)
	}
	for rows.Next() {
		var count int
		err = rows.Scan(&count)

		if count > 0 {
			fmt.Println("there are existing codes in the database.  Skipping code loading.")
			return
		}
	}

	txn, err := db.Begin()
	if err != nil {
		fmt.Printf("Couldn't create transaction: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("codes", "code", "description"))
	if err != nil {
		fmt.Printf("Couldn't create transaction statement: %v", err)
	}

	for _, code := range sampleCodes {
		_, err = stmt.Exec(code[0], code[1])
	}

	_, err = stmt.Exec()
	if err != nil {
		fmt.Printf("Couldn't execute transaction statement: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		fmt.Printf("Couldn't close transaction statement: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		fmt.Printf("Couldn't commit transaction: %v", err)
	}

}

func getSampleCodes() [][]string {
	return [][]string{
		[]string{"1", "Excision eye lesion NOS"},
		[]string{"2", "Dx aspirat-ant chamber"},
		[]string{"3", "C & s-op wound"},
		[]string{"4", "Nonmechan resuscitation"},
		[]string{"5", "Referral for drug rehab"},
		[]string{"6", "Renal diagnost proc NEC"},
		[]string{"7", "Opn mitral valvuloplasty"},
		[]string{"8", "Remov large bowel tube"},
		[]string{"9", "Total body scan"},
		[]string{"10", "Perc abltn liver les/tis"},
		[]string{"11", "Lap sigmoidectomy"},
		[]string{"12", "Nonoperative exams NEC"},
		[]string{"13", "Periren/vesicle excision"},
		[]string{"14", "High forceps w episiot"},
		[]string{"15", "Urinary manometry"},
		[]string{"16", "Occlude leg artery NEC"},
		[]string{"17", "Adrenal nerve division"},
		[]string{"18", "Endo transmyo revascular"},
		[]string{"19", "Tonometry"},
		[]string{"20", "Unilat rad neck dissect"},
		[]string{"21", "Injection into heart"},
		[]string{"22", "Assisting exercise"},
		[]string{"23", "Rib/sternum/clavic x-ray"},
		[]string{"24", "Lumb/lmbsac fus ant/post"},
		[]string{"25", "Vasotomy"},
		[]string{"26", "Revision of lead"},
		[]string{"27", "Insert recombinant BMP"},
		[]string{"28", "Free skin graft NEC"},
		[]string{"29", "Electrocardiograph monit"},
		[]string{"30", "Aspiration skin & subq"},
		[]string{"31", "Remov abdom wall suture"},
		[]string{"32", "Periph nerv anastom NEC"},
		[]string{"33", "Carpal tunnel release"},
		[]string{"34", "Adm neuroprotective agnt"},
		[]string{"35", "Oth endo proc oth vessel"},
		[]string{"36", "Oth remove rem ova/tube"},
		[]string{"37", "Thorac duct cannulation"},
		[]string{"38", "Removal FB spinal canal"},
		[]string{"39", "Remove head/neck dev NEC"},
		[]string{"40", "Reduction genioplasty"},
		[]string{"41", "Open spleen biopsy"},
		[]string{"42", "Trans bal dil pros ureth"},
		[]string{"43", "Oth thorac op thymus NOS"},
		[]string{"44", "Ex cereb meningeal les"},
		[]string{"45", "Percutan aspiration gb"},
		[]string{"46", "Percutan bartholin aspir"},
		[]string{"47", "Suture anal laceration"},
		[]string{"48", "Resection of nose"},
		[]string{"49", "Inc/exc/destr in ear NEC"},
		[]string{"50", "Residual root removal"},
		[]string{"51", "Parasitology-eye"},
		[]string{"52", "Oth chest cage ostectomy"},
		[]string{"53", "Trocar cholecystostomy"},
		[]string{"54", "Endarterectomy of aorta"},
		[]string{"55", "Cataract extraction NEC"},
		[]string{"56", "Esophageal incision NEC"},
		[]string{"57", "Exc/dest hrt les"},
		[]string{"58", "Clos large bowel biopsy"},
		[]string{"59", "Conjunctivoplasty NEC"},
		[]string{"60", "Total ureterectomy"},
		[]string{"61", "Part breech extract NEC"},
		[]string{"62", "Open rectal biopsy"},
		[]string{"63", "Fallopian tube insufflat"},
		[]string{"64", "Other muscle transposit"},
		[]string{"65", "Suture cornea laceration"},
		[]string{"66", "Mri musculoskeletal"},
		[]string{"67", "Tm manipulation NEC"},
		[]string{"68", "Revis cutan ureteros NEC"},
		[]string{"69", "Vaginotomy NEC"},
		[]string{"70", "Replace nephrostomy tube"},
		[]string{"71", "Remov peritoneal drain"},
		[]string{"72", "Implt cardiodefib leads"},
		[]string{"73", "Infundibulectomy"},
		[]string{"74", "Fat graft to breast"},
		[]string{"75", "Culture NEC"},
		[]string{"76", "Oth bone repa/plast NEC"},
		[]string{"77", "Pelvic evisceration"},
		[]string{"78", "Hymenectomy"},
		[]string{"79", "Musc/fasc excis for grft"},
		[]string{"80", "Rad excis ext ear les"},
		[]string{"81", "Other tenotomy"},
		[]string{"82", "Repair eyeball rupture"},
		[]string{"83", "Thorac interposition NEC"},
		[]string{"84", "Endosc destr pancrea les"},
		[]string{"85", "Arterial puncture NEC"},
		[]string{"86", "Procedure-one vessel"},
		[]string{"87", "Mult seg sm bowel excis"},
		[]string{"88", "Exposure of tooth"},
		[]string{"89", "Remov breast tissu expan"},
		[]string{"90", "Other fasciectomy"},
		[]string{"91", "Bact smear-blood"},
		[]string{"92", "Pharyngeal dx proc NEC"},
		[]string{"93", "Open bx saliv gland/duct"},
		[]string{"94", "Cervical spine x-ray NEC"},
		[]string{"95", "Functional pt evaluation"},
		[]string{"96", "Clos thoracic fistul NEC"},
		[]string{"97", "Thomas' splint traction"},
		[]string{"98", "Total body scan"},
		[]string{"99", "Periop aut trans hol bld"},
		[]string{"100", "Part ostect-tibia/fibula"},
	}
}
