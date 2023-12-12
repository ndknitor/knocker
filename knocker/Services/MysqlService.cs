using System.Data;
using System.Text;
using Dapper.Transaction;
using KnCsvReader;
using MySqlConnector;

public class MysqlService : IService
{
    private readonly MySqlConnection connection;
    IEnumerable<string> inputPaths;
    string delimiter;
    IEnumerable<string> excludeTables;
    public MysqlService(MySqlConnection connection, IEnumerable<string> inputPaths, string delimiter, IEnumerable<string> excludeTables)
    {
        this.connection = connection;
        this.inputPaths = inputPaths;
        this.delimiter = delimiter;
        this.excludeTables = excludeTables;
    }
    public void PerformReset()
    {
        if (connection.State == ConnectionState.Closed)
            connection.Open();
        using (IDbTransaction transaction = connection.BeginTransaction())
        {
            try
            {
                transaction.Execute("SET FOREIGN_KEY_CHECKS = 0;");
                DeleteData(transaction);
                InsertData(transaction);
                transaction.Execute("SET FOREIGN_KEY_CHECKS = 1;");
                transaction.Commit();
            }
            catch (System.Exception)
            {
                transaction.Rollback();
                throw;
            }
        }
    }
    private void DeleteData(IDbTransaction transaction)
    {
        string deleteQuery = transaction.QueryFirst<string>(@"SELECT GROUP_CONCAT(CONCAT('DELETE FROM ', table_name) SEPARATOR ';') FROM information_schema.tables WHERE table_schema = @dbname AND TABLE_NAME not in @excludeTables;", new { dbname = connection.Database, excludeTables = excludeTables });
        transaction.Execute(deleteQuery);
    }
    private void InsertData(IDbTransaction transaction)
    {
        foreach (string csvFilePath in inputPaths)
        {
            var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
            string[] columns = null;
            IEnumerable<IDictionary<string, object>> data = Csv.ReadFile(csvFilePath, delimiter);
            if (data.Count() > 0)
            {
                columns = data.ElementAt(0).Keys.ToArray();
                transaction.Execute(InsertCommand(columns, tableName), data);
                Console.WriteLine($"Table {tableName}'s data has been reseted successfully.");
            }
            else
            {
                Console.WriteLine($"No data include for table {tableName}");
                continue;
            }
        }
    }
    private string InsertCommand(string[] columns, string tableName)
    {
        StringBuilder columnsText = new StringBuilder();
        foreach (var item in columns)
        {
            columnsText.Append($"`{item}`,");
        }
        columnsText.Remove(columnsText.Length - 1, 1);
        return $"INSERT INTO {tableName} ({columnsText}) VALUES (@{string.Join(", @", columns)})";
    }
}