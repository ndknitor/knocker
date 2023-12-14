using System.Data;
using System.Data.SqlClient;
using System.Text;
using Dapper.Transaction;
using KnCsvReader;

public class MssqlService : IService
{
    public SqlConnection Connection { get; set; }
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
        transaction.Execute(@"
            EXEC sp_MSForEachTable 'DISABLE TRIGGER ALL ON ?'
            EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");
    }
    private void EnableForeignKeyCheck(IDbTransaction transaction)
    {
        transaction.Execute(@"
            EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL'
            EXEC sp_MSForEachTable 'ENABLE TRIGGER ALL ON ?'");
    }
    private void DeleteData(IDbTransaction transaction)
    {
        IEnumerable<string> tables = transaction.Query<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'dbo' AND TABLE_NAME not in @excludeTables;", new { excludeTables = ExcludeTables });
        StringBuilder deleteQuery = new StringBuilder();
        foreach (string item in tables)
        {
            deleteQuery.AppendLine($"Delete from [{item}]");
        }
        transaction.Execute(deleteQuery.ToString());
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
            columnsText.Append($"[{item}],");
        }
        columnsText.Remove(columnsText.Length - 1, 1);
        return $"INSERT INTO [{tableName}] ({columnsText}) VALUES (@{string.Join(", @", columns)})";
    }
}
