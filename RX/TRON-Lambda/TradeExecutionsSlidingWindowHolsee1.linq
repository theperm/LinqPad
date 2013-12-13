<Query Kind="Program">
  <Connection>
    <ID>8c50f33b-f431-47ce-ab08-85212ab1f0a3</ID>
    <Driver Assembly="StreamInsightLinqPad" PublicKeyToken="3d3a4b0768c9178e">StreamInsightLinqPad.StaticDriver</Driver>
    <CustomAssemblyPath>StreamInsightLinqPad.Samples.dll</CustomAssemblyPath>
    <CustomTypeName>StreamInsightLinqPad.Samples.StreamInsightContext</CustomTypeName>
    <DriverData>
      <ContextKind>StreamInsight</ContextKind>
      <RequiredVersion>21.0.0.0</RequiredVersion>
    </DriverData>
  </Connection>
  <Output>DataGrids</Output>
</Query>


public class TradeExecutionPayload
{
   public override string ToString()
   {
       return String.Format("{{ TradeTime = {0}, ProductId = {1}, FillReason = {2} }}", TradeTime, ProductId, FillReason);
   }

   public DateTime TradeTime { get; internal set; }
   
   public string ProductId { get; internal set; }
   
   public int FillReason { get; internal set; }
   
   public int Quantity { get; internal set; }
}

public class RandomTradeExecutionPayload : TradeExecutionPayload
{
   static string[] ProductMap = new string[] {
       "GEZ3",
       "GEH4",
       "GEM4",
       "GEU4",
       "GEZ4",
       "GEH5",
       "GEM5",
       "GEU5",
       "GEZ5",
       "GEH6",
       "GEM6",
       "GEU6",
       "GEZ6"
   };
    
   public RandomTradeExecutionPayload()
   {
       var rand = new Random();
       TradeTime = DateTime.Now;
       FillReason = rand.Next(0, 9);
       ProductId = ProductMap[rand.Next(0, 13)];
       Quantity = rand.Next(0, 9999);
   }
}

void Main()
{
    TimeSpan inputFrequency = TimeSpan.FromSeconds(.2);
    var tradeExecutionPayloads = Observable.Interval(inputFrequency).Select(e => new RandomTradeExecutionPayload());

       var query = from win in tradeExecutionPayloads.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
                           select win;
       //query.Dump();
       tradeExecutionPayloads.Dump();
}

// Define other methods and classes here






