using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using CsvHelper;
using CsvHelper.Configuration;

namespace CSVRecordMatcher
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // Configuration for Hadoop HDFS
                string hdfsUri = "http://localhost:9870/";
                string hdfsUsername = "adil";
                string hdfsDirectory = "/user";
                string singleCsvFileName = "customer.csv";
                string latestCsvFileName = "latest-customer.csv";

                // Define the HDFS file paths
                string singleCsvFilePath = Path.Combine(hdfsDirectory, singleCsvFileName);
                string latestCsvFilePath = Path.Combine(hdfsDirectory, latestCsvFileName);

                // Update the CSV file
                string updatedFilePath = UpdateCsvFile(singleCsvFilePath, latestCsvFilePath, hdfsUri, hdfsUsername);

                Console.WriteLine("Updated CSV file path: " + updatedFilePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        static string UpdateCsvFile(string singleCsvFilePath, string latestCsvFilePath, string hdfsUri, string hdfsUsername)
        {
            // Read single CSV file from HDFS
            string singleCsvData = ReadCsvFromHdfs(hdfsUri, hdfsUsername, singleCsvFilePath);

            // Read latest CSV file from HDFS
            string latestCsvData = ReadCsvFromHdfs(hdfsUri, hdfsUsername, latestCsvFilePath);

            // Parse the single CSV data
            List<CsvRow> singleRows = ParseCsvData<CsvRow>(singleCsvData, "Customer Id");

            // Parse the latest CSV data
            List<CsvRow> latestRows = ParseCsvData<CsvRow>(latestCsvData, "Customer Id");

            var customerIds = new HashSet<string>();
            var updatedRows = new List<CsvRow>(singleRows);

            foreach (var singleRow in singleRows)
            {
                string customerId = singleRow.CustomerId;
                customerIds.Add(customerId);
            }

            foreach (var latestRow in latestRows)
            {
                string customerId = latestRow.CustomerId;
                var matchingRow = updatedRows.FirstOrDefault(row => row.CustomerId == customerId);
                if (matchingRow != null)
                {
                    matchingRow.UpdateFields(latestRow);
                }
                else
                {
                    updatedRows.Add(latestRow);
                }
            }

            // Generate the updated CSV data
            string updatedCsvData = GenerateCsvData(updatedRows);

            // Write the updated CSV data to a file
            string updatedFilePath = Path.Combine(Directory.GetCurrentDirectory(), "updated_Records.csv");
            File.WriteAllText(updatedFilePath, updatedCsvData);

            return updatedFilePath;
        }

        static string ReadCsvFromHdfs(string hdfsUri, string username, string filePath)
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("Authorization", $"Basic {Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(username))}");

                var response = client.GetAsync($"{hdfsUri}webhdfs/v1{filePath}?op=OPEN").Result;
                if (response.IsSuccessStatusCode)
                {
                    return response.Content.ReadAsStringAsync().Result;
                }
                else
                {
                    throw new Exception($"Failed to read CSV file from HDFS. Status code: {response.StatusCode}, Reason: {response.ReasonPhrase}");
                }
            }
        }

        static List<T> ParseCsvData<T>(string csvData, string idColumnName)
        {
            using (var reader = new StringReader(csvData))
            using (var csv = new CsvReader(reader, System.Globalization.CultureInfo.InvariantCulture))
            {
                csv.Context.RegisterClassMap<CsvRowMap>();

                var records = new List<T>();
                while (csv.Read())
                {
                    var record = csv.GetRecord<T>();
                    records.Add(record);
                }

                return records;
            }
        }

        static string GenerateCsvData(List<CsvRow> rows)
        {
            using (var writer = new StringWriter())
            using (var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture))
            {
                csv.WriteField("Customer Id");
                csv.WriteField("First Name");
                csv.WriteField("Last Name");
                csv.WriteField("Company");
                csv.WriteField("City");
                csv.WriteField("Country");
                csv.WriteField("Phone 1");
                csv.WriteField("Phone 2");
                csv.WriteField("Email");
                csv.WriteField("Subscription Date");
                csv.WriteField("Website");
                csv.NextRecord();

                foreach (var row in rows)
                {
                    csv.WriteRecord(row);
                    csv.NextRecord();
                }

                return writer.ToString();
            }
        }
    }

    public class CsvRow
    {
        public string? CustomerId { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public string? Company { get; set; }
        public string? City { get; set; }
        public string? Country { get; set; }
        public string? Phone1 { get; set; }
        public string? Phone2 { get; set; }
        public string? Email { get; set; }
        public string? SubscriptionDate { get; set; }
        public string? Website { get; set; }

        public void UpdateFields(CsvRow other)
        {
            FirstName = other.FirstName;
            LastName = other.LastName;
            Company = other.Company;
            City = other.City;
            Country = other.Country;
            Phone1 = other.Phone1;
            Phone2 = other.Phone2;
            Email = other.Email;
            SubscriptionDate = other.SubscriptionDate;
            Website = other.Website;
        }
    }

    public sealed class CsvRowMap : ClassMap<CsvRow>
    {
        public CsvRowMap()
        {
            Map(m => m.CustomerId).Name("Customer Id");
            Map(m => m.FirstName).Name("First Name");
            Map(m => m.LastName).Name("Last Name");
            Map(m => m.Company).Name("Company");
            Map(m => m.City).Name("City");
            Map(m => m.Country).Name("Country");
            Map(m => m.Phone1).Name("Phone 1");
            Map(m => m.Phone2).Name("Phone 2");
            Map(m => m.Email).Name("Email");
            Map(m => m.SubscriptionDate).Name("Subscription Date");
            Map(m => m.Website).Name("Website");
        }
    }
}
