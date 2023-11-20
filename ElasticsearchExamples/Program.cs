using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elasticsearch.Net;
using Nest;

namespace ElasticsearchExamples
{
    internal class Program
    {
        const string IndexName = "stock-demo-v1";
        const string AliasName = "stock-demo";
    //  public static ElasticsearchClient Client =>  new ElasticsearchClient( new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
    //         .CertificateFingerprint("44b2fa34ad571bc23f6e80e1355656192b2fc7b6")
    //         .Authentication(new BasicAuthentication("elastic", "Elastic1!")));//.IndexAsync(IndexName);

        static SingleNodeConnectionPool pool = new SingleNodeConnectionPool(new Uri("https://localhost:9200"));

        static ConnectionSettings settings = new ConnectionSettings(pool)
           // .CertificateFingerprint("44:b2:fa:34:ad:57:1b:c2:3f:6e:80:e1:35:56:56:19:2b:2f:c7:b6")
            .BasicAuthentication("elastic", "Elastic1!")
            .EnableApiVersioningHeader();
      //  public static IElasticClient Client = new ElasticClient(new ConnectionSettings().DefaultIndex(IndexName));
  public static IElasticClient Client = new ElasticClient(settings);

        public static ConnectionSettings Settings { get => settings; set => settings = value; }

        private static async Task Main(string[] args)
        {
            var existsResponse = await Client.Indices.ExistsAsync(IndexName);
            var sss=ReadStockData();
            var sd=sss.Count();
            if (!existsResponse.Exists)
            {
                var newIndexResponse = await Client.Indices.CreateAsync(IndexName, i => i
                    .Map<StockData>(m => m
                        .AutoMap<StockData>()
                        .Properties<StockData>(p => p.Keyword(k => k.Name(f => f.Symbol))))
                    .Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));
                if (!newIndexResponse.IsValid || newIndexResponse.Acknowledged is false) throw new Exception("Oh no!!");

                var bulkAll = Client.BulkAll(ReadStockData(), r => r
                    .Index(IndexName)
                    .BackOffRetries(2)
                    .BackOffTime("30s")
                    .MaxDegreeOfParallelism(4)
                    .Size(1000));

                bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));

                var aliasResponse = await Client.Indices.PutAliasAsync(IndexName, AliasName);
                if (!aliasResponse.IsValid) throw new Exception("Oh no!!");
            }
        }

        public static IEnumerable<StockData> ReadStockData()
        {
            // Update this to the correct path of the CSV file
            var file = new StreamReader("C:\\Nauka\\elasticsearch-examples\\all_stocks_5yr.csv", new FileStreamOptions{
                Access=FileAccess.Read
            });

            string line;
            while ((line = file.ReadLine()) is not null) yield return new StockData(line);
        }
    }

    public class StockData
    {
        private static readonly Dictionary<string, string> CompanyLookup = new()
        {
            {"AAL", "American Airlines Group Inc"},
            {"MSFT", "Microsoft Corporation"},
            {"AME", "AMETEK, Inc."},
            {"M", "Macy's Inc"}
        };

        public StockData(string dataLine)
        {
            var columns = dataLine.Split(',', StringSplitOptions.TrimEntries);

            if (DateTime.TryParse(columns[0], out var date))
                Date = date;

            if (double.TryParse(columns[1], out var open))
                Open = open;

            if (double.TryParse(columns[2], out var high))
                High = high;

            if (double.TryParse(columns[3], out var low))
                Low = low;

            if (double.TryParse(columns[4], out var close))
                Close = close;

            if (uint.TryParse(columns[5], out var volume))
                Volume = volume;

            Symbol = columns[6];

            if (CompanyLookup.TryGetValue(Symbol, out var name))
                Name = name;
        }

        public DateTime Date { get; init; }
        public double Open { get; init; }
        public double Close { get; init; }
        public double High { get; init; }
        public double Low { get; init; }
        public uint Volume { get; init; }
        public string Symbol { get; init; }
        public string Name { get; init; }
    }
}