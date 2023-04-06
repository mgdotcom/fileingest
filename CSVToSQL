using System;
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
                            dataRow[i] = fields[i];
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
                                                                    string.Join(", ", headers) + ")", connection);
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
    }
}
