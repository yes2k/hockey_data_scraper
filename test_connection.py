import mysql.connector



if __name__ == "__main__":
  try:
    con = mysql.connector.connect(
      host="test-mysql-db",
      user="root",
      password="password",
      # database = "db"
      
    )
    print("Connection successful.")
  except mysql.connector.Error as err:
    print(f"Error: {err}")

  c = con.cursor()
  c.execute("select 1;")
  print(c.fetchall())