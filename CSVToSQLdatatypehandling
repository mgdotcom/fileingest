using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace CSVToSQL
{
    class Program
    {
        static void Main(string[] args)
        {
            // Replace with your SQL server connection string
            string connectionString = "Data Source=(local);Initial Catalog=YourDatabase;Integrated Security=True";

            // Replace with the path to your CSV or text file
            string filePath = "C:\\path\\to\\your\\file.csv";

            try
            {
                // Read the CSV or text file into a DataTable
                DataTable dataTable = new DataTable();
                using (StreamReader reader = new StreamReader(filePath))
                {
                    string[] headers = reader.ReadLine().Split(',');
                    foreach (string header in headers)
                    {
                        dataTable.Columns.Add(header);
                    }

                    while (!reader.EndOfStream)
                    {
                        string[] fields = reader.ReadLine().Split(',');
                        DataRow dataRow = dataTable.NewRow();
                        for (int i = 0; i < headers.Length; i++)
                        {
                            dataRow[i] = ConvertField(fields[i], dataTable.Columns[i].DataType);
                        }
                        dataTable.Rows.Add(dataRow);
                    }
                }

                // Connect to the SQL server and create a table based on the column headers in the file
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    SqlCommand createTableCommand = new SqlCommand("CREATE TABLE YourTableName (" +
                                                                    "id INT IDENTITY(1,1) PRIMARY KEY," +
                                                                    GetColumnDefinitions(dataTable) + ")", connection);
                    createTableCommand.ExecuteNonQuery();

                    // Ingest the data from the file into the table
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = "YourTableName";
                        bulkCopy.WriteToServer(dataTable);
                    }

                    Console.WriteLine("File successfully ingested into SQL server.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred: " + ex.Message);
            }
        }

        // Convert the field to the specified data type
        static object ConvertField(string field, Type dataType)
        {
            if (string.IsNullOrEmpty(field))
            {
                return DBNull.Value;
            }

            object value = null;

            if (dataType == typeof(string))
            {
                value = field;
            }
            else if (dataType == typeof(int))
            {
                int.TryParse(field, out int intValue);
                value = intValue;
            }
            else if (dataType == typeof(decimal))
            {
                decimal.TryParse(field, out decimal decimalValue);
                value = decimalValue;
            }
            else if (dataType == typeof(DateTime))
            {
                DateTime.TryParse(field, out DateTime dateTimeValue);
                value = dateTimeValue;
            }

            return value;
        }

        // Get a comma-separated string of column definitions for the CREATE TABLE statement
        static string GetColumnDefinitions(DataTable dataTable)
        {
            List<string> columnDefinitions = new List<string>();

            foreach (DataColumn column in dataTable.Columns)
            {
                string columnDefinition = column.ColumnName + " " + GetSqlDataType(column.DataType);
                columnDefinitions.Add(columnDefinition);
            }

            return string.Join(", ", columnDefinitions);
        }

        // Get the SQL data type equivalent of the specified .NET data type
        static string GetSqlDataType(Type dataType)
        {
            if (dataType == typeof(string))
            {
                return "NVARCHAR(MAX)";
            }
            else if (dataType == typeof(int))
            {
                return "INT";
            }
            else if (dataType == typeof(decimal))
            {
                return
