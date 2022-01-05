package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func schedule(db *sql.DB) {
	endSpot := time.Now()
	endHourly := time.Now()
	endTwh := time.Now()
	endTfh := time.Now()
	endElite := time.Now()
	startSpot := time.Now()
	var durationSpot int
	updateSpot := time.Now()
	var intervalSpot int
	startHourly := time.Now()
	var durationHourly int
	updateHourly := time.Now()
	var intervalHourly int
	startTwh := time.Now()
	var durationTwh int
	startTfh := time.Now()
	var durationTfh int
	updateTwh := time.Now()
	updateTfh := time.Now()
	var intervalTwh int
	var intervalTfh int
	var durationElite int
	var intervalElite int
	startElite := time.Now()
	updateElite := time.Now()

	var count int
	var id int
	var ref string
	var category string
	var prod_name string
	var desc string
	var mrp int
	out1 := "insert into spottable values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
	out2 := "insert into hourlytable values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
	out3 := "insert into twelvehourtable values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
	out4 := "insert into twentyfourhourtable values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
	out5 := "insert into elitetable values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"

	rowLine := db.QueryRow("select * from logic where id=$1", 1)
	err1 := rowLine.Scan(&id, &count, &startSpot, &durationSpot, &intervalSpot, &endSpot)
	if err1 != nil {
		log.Fatal(err1)
	}

	rowLine2 := db.QueryRow("select * from logic where id=$1", 2)
	err2 := rowLine2.Scan(&id, &count, &startHourly, &durationHourly, &intervalHourly, &endHourly)
	if err2 != nil {
		log.Fatal(err2)
	}

	rowLine3 := db.QueryRow("select * from logic where id=$1", 3)
	err3 := rowLine3.Scan(&id, &count, &startTwh, &durationTwh, &intervalTwh, &endTwh)
	if err3 != nil {
		log.Fatal(err3)
	}

	rowLine4 := db.QueryRow("select * from logic where id=$1", 4)
	err4 := rowLine4.Scan(&id, &count, &startTfh, &durationTfh, &intervalTfh, &endTfh)
	if err4 != nil {
		log.Fatal(err4)
	}

	rowLine6 := db.QueryRow("select * from logic where id=$1", 5)
	err6 := rowLine6.Scan(&id, &count, &startElite, &durationElite, &intervalElite, &endElite)
	if err6 != nil {
		log.Fatal(err6)
	}

	rows, err5 := db.Query("select * from inputtable order by random()")
	if err5 != nil {
		log.Fatal(err5)
	}

	spotIndex := 1
	spotAuction := 0
	hourIndex := 1
	hourAuction := 0
	twelveIndex := 1
	twelveAuction := 0
	twentyFourIndex := 1
	twentyFourAuction := 0
	eliteIndex := 1
	eliteAuction := 0
	sf := true
	hf := true
	twf := true
	tff := true
	ef := true

	for rows.Next() {
		err := rows.Scan(&ref, &category, &prod_name, &desc, &mrp)

		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("spot: ", spotIndex, " hour: ", hourIndex, "twelve: ", twelveIndex, "twentyFour: ", twentyFourIndex, "elite: ", eliteIndex)

		if mrp >= 1 && mrp <= 999 {
			// if int(startSpot.Month()) >= int(endSpot.Month()) && startSpot.Day() >= endSpot.Day() {
			// 	if startSpot.Year() >= endSpot.Year() && startSpot.Hour() >= endSpot.Hour() {
			// 		sf = false
			// 		continue
			// 	}
			// }

			if startSpot.After(endSpot) {
				sf = false
				continue
			}

			if startSpot.Hour() >= 0 && startSpot.Hour() < 9 {
				nt := startSpot.String()
				ntl := strings.Split(nt, " ")
				ntl[1] = "09:00:00"
				ntl = ntl[:2]
				nt = ntl[0] + "T" + ntl[1] + "+00:00"
				start2, _ := time.Parse("2006-01-02T15:04:05Z07:00", nt)
				startSpot = start2

			}

			updateSpot = startSpot.Add(time.Duration(durationSpot) * time.Minute)
			base := 0.99 * float64(mrp)
			err, _ := db.Exec(out1, spotIndex, spotAuction, startSpot, updateSpot, 1, 1, ref, category, prod_name, desc, mrp, 1, int(base))

			if err != nil {
				fmt.Println(err)
			}

			if spotIndex%count == 0 {
				spotAuction++
				startSpot = startSpot.Add(time.Duration(intervalSpot) * time.Minute)
			}

			spotIndex++

		} else if mrp >= 1000 && mrp <= 2999 {
			// if int(startHourly.Month()) >= int(endHourly.Month()) && startHourly.Day() >= endHourly.Day() {
			// 	if startHourly.Year() >= endHourly.Year() && startHourly.Hour() >= endHourly.Hour() {
			// 		hf = false
			// 		continue
			// 	}
			// }

			if startHourly.After(endHourly) {
				hf = false
				continue
			}

			if startHourly.Hour() >= 0 && startHourly.Hour() < 9 {
				nt := startHourly.String()
				ntl := strings.Split(nt, " ")
				ntl[1] = "09:00:00"
				ntl = ntl[:2]
				nt = ntl[0] + "T" + ntl[1] + "+00:00"
				start2, _ := time.Parse("2006-01-02T15:04:05Z07:00", nt)
				startHourly = start2

			}
			updateHourly = startHourly.Add(time.Duration(durationHourly) * time.Minute)
			base := 0.99 * float64(mrp)
			err, _ := db.Exec(out2, hourIndex, hourAuction, startHourly, updateHourly, 1, 1, ref, category, prod_name, desc, mrp, 1, int(base))

			if err != nil {
				fmt.Println(err)
			}

			if hourIndex%count == 0 {
				hourAuction++
				startHourly = startHourly.Add(time.Duration(intervalHourly) * time.Minute)
			}

			hourIndex++

		} else if mrp >= 3000 && mrp <= 4999 {
			// if int(startTwh.Month()) >= int(endTwh.Month()) && startTwh.Day() >= endTwh.Day() {
			// 	if startTwh.Year() >= endTwh.Year() && startTwh.Hour() >= endTwh.Hour() {
			// 		twf = false
			// 		continue
			// 	}
			// }

			if startTwh.After(endTwh) {
				twf = false
				continue
			}

			updateTwh = startTwh.Add(time.Duration(durationTwh) * time.Minute)
			base := 0.99 * float64(mrp)
			err, _ := db.Exec(out3, twelveIndex, twelveAuction, startTwh, updateTwh, 1, 1, ref, category, prod_name, desc, mrp, 1, int(base))

			if err != nil {
				fmt.Println(err)
			}

			if twelveIndex%count == 0 {
				twelveAuction++
				startTwh = startTwh.Add(time.Duration(intervalTwh) * time.Minute)
			}

			twelveIndex++

		} else if mrp >= 5000 && mrp <= 9999 {
			// if int(startTfh.Month()) >= int(endTfh.Month()) && startTfh.Day() >= endTfh.Day() {
			// 	if startTfh.Year() >= endTfh.Year() && startTfh.Hour() >= endTfh.Hour() {
			// 		tff = false
			// 		continue
			// 	}
			// }
			if startTfh.After(endTfh) {
				tff = false
				continue
			}

			updateTfh = startTfh.Add(time.Duration(durationTfh) * time.Minute)
			base := 0.99 * float64(mrp)
			err, _ := db.Exec(out4, twentyFourIndex, twentyFourAuction, startTfh, updateTfh, 1, 1, ref, category, prod_name, desc, mrp, 1, int(base))
			if err != nil {
				fmt.Println(err)
			}

			if twentyFourIndex%count == 0 {
				twentyFourAuction++
				startTfh = startTfh.Add(time.Duration(intervalTfh) * time.Minute)
			}

			twentyFourIndex++

		} else if mrp >= 10000 && mrp <= 24999 {
			// if int(startTfh.Month()) >= int(endTfh.Month()) && startTfh.Day() >= endTfh.Day() {
			// 	if startTfh.Year() >= endTfh.Year() && startTfh.Hour() >= endTfh.Hour() {
			// 		tff = false
			// 		continue
			// 	}
			// }
			if startElite.After(endElite) {
				ef = false
				continue
			}

			updateElite = startElite.Add(time.Duration(durationElite) * time.Minute)
			base := 0.99 * float64(mrp)
			err, _ := db.Exec(out5, eliteIndex, eliteAuction, startElite, updateElite, 1, 1, ref, category, prod_name, desc, mrp, 1, int(base))
			if err != nil {
				fmt.Println(err)
			}

			if eliteIndex%count == 0 {
				eliteAuction++
				startElite = startElite.Add(time.Duration(intervalElite) * time.Minute)
			}

			eliteIndex++

		}

	}
	var tbase int
	for sf {
		spotrows, spoterr := db.Query("select ref_id,category,prod_name,description,mrp,base from spottable order by random();")
		if spoterr != nil {
			log.Fatal("error selecting from spottable", spoterr)
		}
		for spotrows.Next() {
			fmt.Println("spot: ", spotIndex)
			serr := spotrows.Scan(&ref, &category, &prod_name, &desc, &mrp, &tbase)
			if serr != nil {
				log.Fatal("error scanning spot", serr)
			}
			// if int(startSpot.Month()) >= int(endSpot.Month()) && startSpot.Day() >= endSpot.Day() {
			// 	if startSpot.Year() >= endSpot.Year() && startSpot.Hour() >= endSpot.Hour() {
			// 		sf = false
			// 		break
			// 	}
			// }
			if startSpot.After(endSpot) {
				sf = false
				break
			}

			if startSpot.Hour() >= 0 && startSpot.Hour() < 9 {
				nt := startSpot.String()
				ntl := strings.Split(nt, " ")
				ntl[1] = "09:00:00"
				ntl = ntl[:2]
				nt = ntl[0] + "T" + ntl[1] + "+00:00"
				start2, _ := time.Parse("2006-01-02T15:04:05Z07:00", nt)
				startSpot = start2

			}
			updateSpot = startSpot.Add(time.Duration(durationSpot) * time.Minute)
			err, _ := db.Exec(out1, spotIndex, spotAuction, startSpot, updateSpot, 1, 1, ref, category, prod_name, desc, mrp, 1, tbase)

			if err != nil {
				fmt.Println(err)
			}

			if spotIndex%count == 0 {
				spotAuction++
				startSpot = startSpot.Add(time.Duration(intervalSpot) * time.Minute)
			}

			spotIndex++

		}

	}

	for hf {
		hourrows, hourerr := db.Query("select ref_id,category,prod_name,description,mrp,base from hourlytable order by random();")
		if hourerr != nil {
			log.Fatal("error selecting from hourlytable", hourerr)
		}
		for hourrows.Next() {
			fmt.Println("hour: ", hourIndex)
			herr := hourrows.Scan(&ref, &category, &prod_name, &desc, &mrp, &tbase)
			if herr != nil {
				log.Fatal("error scanning hour", herr)
			}
			// if int(startHourly.Month()) >= int(endHourly.Month()) && startHourly.Day() >= endHourly.Day() {
			// 	if startHourly.Year() >= endHourly.Year() && startHourly.Hour() >= endHourly.Hour() {
			// 		hf = false
			// 		break
			// 	}
			// }
			if startHourly.After(endHourly) {
				hf = false
				break
			}
			if startHourly.Hour() >= 0 && startHourly.Hour() < 9 {
				nt := startHourly.String()
				ntl := strings.Split(nt, " ")
				ntl[1] = "09:00:00"
				ntl = ntl[:2]
				nt = ntl[0] + "T" + ntl[1] + "+00:00"
				start2, _ := time.Parse("2006-01-02T15:04:05Z07:00", nt)
				startHourly = start2

			}
			updateHourly = startHourly.Add(time.Duration(durationHourly) * time.Minute)
			err, _ := db.Exec(out2, hourIndex, hourAuction, startHourly, updateHourly, 1, 1, ref, category, prod_name, desc, mrp, 1, tbase)

			if err != nil {
				fmt.Println(err)
			}

			if hourIndex%count == 0 {
				hourAuction++
				startHourly = startHourly.Add(time.Duration(intervalHourly) * time.Minute)
			}

			hourIndex++

		}

	}

	for twf {
		twelverows, twelveerr := db.Query("select ref_id,category,prod_name,description,mrp,base from twelvehourtable where category!=$1 order by random();", "Apparel")
		if twelveerr != nil {
			log.Fatal("error selecting from twelvetable", twelveerr)
		}
		for twelverows.Next() {
			fmt.Println("12 hour: ", twelveIndex)
			twerr := twelverows.Scan(&ref, &category, &prod_name, &desc, &mrp, &tbase)
			if twerr != nil {
				log.Fatal("error scanning twelve", twerr)
			}
			// if int(startTwh.Month()) >= int(endTwh.Month()) && startTwh.Day() >= endTwh.Day() {
			// 	if startTwh.Year() >= endTwh.Year() && startTwh.Hour() >= endTwh.Hour() {
			// 		twf = false
			// 		break
			// 	}
			// }
			if startTwh.After(endTwh) {
				twf = false
				break
			}

			updateTwh = startTwh.Add(time.Duration(durationTwh) * time.Minute)
			err, _ := db.Exec(out3, twelveIndex, twelveAuction, startTwh, updateTwh, 1, 1, ref, category, prod_name, desc, mrp, 1, tbase)

			if err != nil {
				fmt.Println(err)
			}

			if twelveIndex%count == 0 {
				twelveAuction++
				startTwh = startTwh.Add(time.Duration(intervalTwh) * time.Minute)
			}

			twelveIndex++

		}

	}

	for tff {
		twentyrows, twentyerr := db.Query("select ref_id,category,prod_name,description,mrp,base from twentyfourhourtable order by random();")
		if twentyerr != nil {
			log.Fatal("error selecting from twentytable", twentyerr)
		}
		for twentyrows.Next() {
			fmt.Println("24 hour: ", twentyFourIndex)
			tferr := twentyrows.Scan(&ref, &category, &prod_name, &desc, &mrp, &tbase)
			if tferr != nil {
				log.Fatal("error scanning twenty", tferr)
			}
			// if int(startTfh.Month()) >= int(endTfh.Month()) && startTfh.Day() >= endTfh.Day() {
			// 	if startTfh.Year() >= endTfh.Year() && startTfh.Hour() >= endTfh.Hour() {
			// 		tff = false
			// 		break
			// 	}
			// }

			if startTfh.After(endTfh) {
				tff = false
				break
			}

			updateTfh = startTfh.Add(time.Duration(durationTfh) * time.Minute)
			err, _ := db.Exec(out4, twentyFourIndex, twentyFourAuction, startTfh, updateTfh, 1, 1, ref, category, prod_name, desc, mrp, 1, tbase)
			if err != nil {
				fmt.Println(err)
			}

			if twentyFourIndex%count == 0 {
				twentyFourAuction++
				startTfh = startTfh.Add(time.Duration(intervalTfh) * time.Minute)
			}

			twentyFourIndex++

		}

	}

	for ef {
		eliterows, eliteerr := db.Query("select ref_id,category,prod_name,description,mrp,base from elitetable order by random();")
		if eliteerr != nil {
			log.Fatal("error selecting from elitetable", eliteerr)
		}
		for eliterows.Next() {
			fmt.Println("elite : ", eliteIndex)
			tferr := eliterows.Scan(&ref, &category, &prod_name, &desc, &mrp, &tbase)
			if tferr != nil {
				log.Fatal("error scanning elite", tferr)
			}
			// if int(startTfh.Month()) >= int(endTfh.Month()) && startTfh.Day() >= endTfh.Day() {
			// 	if startTfh.Year() >= endTfh.Year() && startTfh.Hour() >= endTfh.Hour() {
			// 		tff = false
			// 		break
			// 	}
			// }

			if startElite.After(endElite) {
				ef = false
				break
			}

			updateElite = startElite.Add(time.Duration(durationElite) * time.Minute)
			err, _ := db.Exec(out5, eliteIndex, eliteAuction, startElite, updateElite, 1, 1, ref, category, prod_name, desc, mrp, 1, tbase)
			if err != nil {
				fmt.Println(err)
			}

			if eliteIndex%count == 0 {
				eliteAuction++
				startElite = startElite.Add(time.Duration(intervalElite) * time.Minute)
			}

			eliteIndex++

		}

	}

}
func main() {
	err1 := godotenv.Load(".env")
	if err1 != nil {
		log.Fatal(err1)
	}
	db, err := sql.Open("postgres", os.Getenv("POSTGRES_URL"))
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}

	fmt.Println("Database student opened and ready.")
	schedule(db)
}
