package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

const (
	DB_HOST     = "localhost"
	DB_PORT     = "5432"
	DB_USER     = "koinworks"
	DB_PASSWORD = ""
	DB_NAME     = "db_go_sql"
)

func main() {
	db, err := connectDB()

	if err != nil {
		panic(err)
	}

	fmt.Println("DB CONNECTED")
	fmt.Println()

	// create employee
	emp := Employee{
		Email:    "andi@koinworks.com",
		Fullname: "andi",
		Age:      30,
		Division: "Marketing",
	}
	err = createEmployee(db, &emp)
	if err != nil {
		fmt.Println("error :", err.Error())
		return
	}

	// get all employees
	employees, err := getAllEmployees(db)
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}
	for _, employee := range *employees {
		employee.Print()
	}

	// get employee by id
	employeeID := 1
	fmt.Println("\nGet Employee ID", employeeID)
	employee, err := getEmployeeByID(db, employeeID)
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}
	employee.Print()

	// update employee by id
	employeeID = 1
	requestUpdateEmployee := Employee{
		Email:    "abdul.update@koinworks.com",
		Fullname: "Abdul update",
		Age:      31,
		Division: "Marketing",
	}
	err = updateEmployeeByID(db, employeeID, &requestUpdateEmployee)
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}
	fmt.Println("\nUpdated Employee id", employeeID)
	newEmployee, _ := getEmployeeByID(db, employeeID)
	newEmployee.Print()

	// delete employee by id
	employeeID = 2

	err = deleteEmployeeByID(db, employeeID)
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}

	fmt.Printf("employee with id %d deleted\n", employeeID)
}

func connectDB() (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}

	// defer db.Close()

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// connection full
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(10 * time.Second)
	db.SetConnMaxLifetime(10 * time.Second)

	return db, nil
}

type Employee struct {
	ID       int    `json:"id"`
	Fullname string `json:"full_name"`
	Email    string `json:"email"`
	Age      int    `json:"age"`
	Division string `json:"division"`
}

func (e *Employee) Print() {
	fmt.Println("ID :", e.ID)
	fmt.Println("Fullname :", e.Fullname)
	fmt.Println("Email :", e.Email)
	fmt.Println("Age :", e.Age)
	fmt.Println("Division :", e.Division)
	fmt.Println()
}

func getAllEmployees(db *sql.DB) (*[]Employee, error) {
	query := `
		SELECT
			id,
			full_name,
			email,
			age,
			division
		FROM employees
	`

	smt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer smt.Close()

	var employees []Employee

	rows, err := smt.Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		employee := Employee{}

		err := rows.Scan(
			&employee.ID,
			&employee.Fullname,
			&employee.Email,
			&employee.Age,
			&employee.Division,
		)

		if err != nil {
			return nil, err
		}

		employees = append(employees, employee)
	}
	return &employees, nil
}

func createEmployee(db *sql.DB, request *Employee) error {
	query := `
		INSERT INTO employees(full_name, email, age, division)
		VALUES($1, $2, $3, $4)
	`

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(request.Fullname, request.Email, request.Age, request.Division)

	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}

		return err
	}

	return tx.Commit()
}

func getEmployeeByID(db *sql.DB, id int) (*Employee, error) {
	query := `
		SELECT
			id,
			full_name,
			email,
			age,
			division
		FROM employees
		WHERE id=$1
	`

	smt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer smt.Close()

	employee := Employee{}

	row := smt.QueryRow(id)

	err = row.Scan(
		&employee.ID,
		&employee.Fullname,
		&employee.Email,
		&employee.Age,
		&employee.Division,
	)

	if err != nil {
		tempError := fmt.Sprintf("employee with id %d not found", id)
		return nil, errors.New(tempError)
	}

	return &employee, nil
}

func updateEmployeeByID(db *sql.DB, id int, request *Employee) error {
	_, err := getEmployeeByID(db, id)
	if err != nil {
		return err
	}

	query := `
		UPDATE employees
		SET full_name=$1, email=$2, age=$3, division=$4
		WHERE id=$5
	`

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		request.Fullname,
		request.Email,
		request.Age,
		request.Division,
		id,
	)

	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}

		return err
	}

	return tx.Commit()
}

func deleteEmployeeByID(db *sql.DB, id int) error {
	_, err := getEmployeeByID(db, id)
	if err != nil {
		return err
	}

	query := `
		DELETE FROM employees
		WHERE id=$1
	`

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(id)

	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}

		return err
	}

	return tx.Commit()
}
