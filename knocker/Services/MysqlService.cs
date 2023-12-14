using System.Data;
using System.Text;
using Dapper.Transaction;
using KnCsvReader;
using MySqlConnector;

public class MysqlService : IService
{
    public MySqlConnection Connection { get; set; }
    public IEnumerable<string> InputPaths { get; set; }
    public string Delimiter { get; set; }
    public IEnumerable<string> ExcludeTables { get; set; }
    public void PerformReset()
    {
        if (Connection.State == ConnectionState.Closed)
            Connection.Open();
        using (IDbTransaction transaction = Connection.BeginTransaction())
        {
            try
            {
                DisableForeignKeyCheck(transaction);
                DeleteData(transaction);
                InsertData(transaction);
                EnableForeignKeyCheck(transaction);
                transaction.Commit();
            }
            catch (System.Exception)
            {
                transaction.Rollback();
                throw;
            }
        }
    }
    public void PerformDelete()
    {
        if (Connection.State == ConnectionState.Closed)
            Connection.Open();
        using (IDbTransaction transaction = Connection.BeginTransaction())
        {
            try
            {
                DisableForeignKeyCheck(transaction);
                DeleteData(transaction);
                EnableForeignKeyCheck(transaction);
                transaction.Commit();
            }
            catch (System.Exception)
            {
                transaction.Rollback();
                throw;
            }
        }
    }
    private void DisableForeignKeyCheck(IDbTransaction transaction)
    {
        transaction.Execute("SET FOREIGN_KEY_CHECKS = 0;");

    }
    private void EnableForeignKeyCheck(IDbTransaction transaction)
    {
        transaction.Execute("SET FOREIGN_KEY_CHECKS = 1;");
    }
    private void DeleteData(IDbTransaction transaction)
    {
        string deleteQuery = transaction.QueryFirst<string>(@"SELECT GROUP_CONCAT(CONCAT('DELETE FROM ', table_name) SEPARATOR ';') FROM information_schema.tables WHERE table_schema = @dbname AND TABLE_NAME not in @excludeTables;", new { dbname = Connection.Database, excludeTables = ExcludeTables });
        transaction.Execute(deleteQuery);
    }
    private void InsertData(IDbTransaction transaction)
    {
        foreach (string csvFilePath in InputPaths)
        {
            var tableName = new FileInfo(csvFilePath).Name.Split('.')[0];
            string[] columns = null;
            IEnumerable<IDictionary<string, object>> data = Csv.ReadFile(csvFilePath, Delimiter);
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