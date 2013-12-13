<Query Kind="Program">
  <Output>DataGrids</Output>
  <NuGetReference>Ix-Main</NuGetReference>
  <NuGetReference>Rx-Main</NuGetReference>
  <Namespace>System.Reactive.Linq</Namespace>
</Query>

[DebuggerDisplay("\\{ TradeTime = {TradeTime}, ProductId = {ProductId}, FillReason = {FillReason} \\}")]
    public class TradeExecutionPayload : IEquatable<TradeExecutionPayload>
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="TradeExecutionPayload"/> class.
        /// </summary>
        public TradeExecutionPayload()
        {
            TradeTime = DateTime.MinValue;
            ProductId = String.Empty;
            FillReason = 0;
        }

        public TradeExecutionPayload(DateTime tradeTime, string productId, int fillReason, int quantity)
        {
            this.TradeTime = tradeTime;
            this.ProductId = productId;
            this.FillReason = fillReason;
            this.Quantity = quantity;
        }

        public override bool Equals(object obj)
        {
            if (obj is TradeExecutionPayload)
                return Equals((TradeExecutionPayload)obj);
            return false;
        }
        public bool Equals(TradeExecutionPayload obj)
        {
            if (obj == null)
                return false;
            if (!EqualityComparer<DateTime>.Default.Equals(TradeTime, obj.TradeTime))
                return false;
            if (!EqualityComparer<string>.Default.Equals(ProductId, obj.ProductId))
                return false;
            if (!EqualityComparer<int>.Default.Equals(FillReason, obj.FillReason))
                return false;
            return true;
        }
        public override int GetHashCode()
        {
            int hash = 0;
            hash ^= EqualityComparer<DateTime>.Default.GetHashCode(TradeTime);
            hash ^= EqualityComparer<string>.Default.GetHashCode(ProductId);
            hash ^= EqualityComparer<int>.Default.GetHashCode(FillReason);
            return hash;
        }
        public override string ToString()
        {
            return String.Format("{{ TradeTime = {0}, ProductId = {1}, FillReason = {2} }}", TradeTime, ProductId, FillReason);
        }

        //[CsvColumn(FieldIndex = 1, OutputFormat = "O")]
        public DateTime TradeTime { get; internal set; }
        
        //[CsvColumn(FieldIndex = 2)]
        public string ProductId { get; internal set; }
        
        //[CsvColumn(FieldIndex = 3)]
        public int FillReason { get; internal set; }
        
        //[CsvColumn(FieldIndex = 4)]
        public int Quantity { get; internal set; }
		
		public int Index { get; internal set; }
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
        
		static int x ;
		static int fr = 1;
        public RandomTradeExecutionPayload()
        {
			
			x++;
			fr = -fr;
            var rand = new Random();
            TradeTime = DateTime.Now;
            FillReason = fr; //rand.Next(0, 3); // 9
            ProductId = ProductMap[rand.Next(0, 2)]; // 13
            Quantity = x; //rand.Next(1, 3);
			Index = x;
        }
    }
	
void Main()
{
	var trades = Observable.Generate(
		new RandomTradeExecutionPayload(),
		x => true,
		x => new RandomTradeExecutionPayload(), // - 0.5 + random.NextDouble(), 
		x => x,
		x => TimeSpan.FromSeconds(0.4)).Replay().RefCount();

	//var windows = trades.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1)).Publish().RefCount();
	//windows.Dump("Windows");

	var grouped = trades.GroupBy(t => t.ProductId).Replay().RefCount();
	grouped.Dump();

	var windows = from g in grouped
		from win in g.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
		select new { win, Vol = win.Sum(x => x.Quantity) };
	windows.Dump();
	
	var query1 = from g in grouped
		from window in g.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
		let win = window.Publish().RefCount()
		from aggs in Observable.Zip(
			win.Sum(x => x.Quantity),
			//win.GroupBy(x => x.FillReason).SelectMany(x => x, .Sum(x => x.Quantity),
			(from frg in win.GroupBy(x => x.FillReason)
			from fr in frg.Sum(x => x.Quantity)
			select new { FR = frg.Key, Vol = fr } )
			.ToDictionary(x => x.FR, x=> x.Vol),
			(pvol,frvol) => new {
					pvol,
					frvol = frvol.ToDictionary(x => x.Key, v => new { Vol = v.Value, PcVol = (v.Value/(double)pvol) * 100 })
				}
			)
		select new { 
			g.Key, 
			PVol = aggs.pvol,
			FRVol = aggs.frvol
			//Window = aggs.wind 
			};
	
		query1.Dump();
}