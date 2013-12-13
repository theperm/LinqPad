<Query Kind="Program">
  <Output>DataGrids</Output>
  <NuGetReference>Ix-Main</NuGetReference>
  <NuGetReference>Rx-Main</NuGetReference>
  <Namespace>System</Namespace>
  <Namespace>System.Linq</Namespace>
  <Namespace>System.Reactive</Namespace>
  <Namespace>System.Reactive.Concurrency</Namespace>
  <Namespace>System.Reactive.Disposables</Namespace>
  <Namespace>System.Reactive.Joins</Namespace>
  <Namespace>System.Reactive.Linq</Namespace>
  <Namespace>System.Reactive.PlatformServices</Namespace>
  <Namespace>System.Reactive.Subjects</Namespace>
  <Namespace>System.Reactive.Threading.Tasks</Namespace>
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
		x => TimeSpan.FromSeconds(0.4)).Publish().RefCount();

	var windows = trades.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1)).Publish().RefCount();
	windows.Dump("Windows");
	
	//var winthenGrp = from window in windows //trades.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
	//	from productGrp in window.GroupBy(x => x.ProductId)
	//	select productGrp;
		//from frGrp in product.GroupBy(ex => ex.FillReason)
		//select frGrp; //.Select(x => x.Quantity).Sum().Take(1);
	var winthenGrp = windows.SelectMany(window => 
		window.GroupBy(x => x.ProductId));
	
	
	var winthenGrpSum2 = windows.SelectMany(window => 
			(from pgrp in window.GroupBy(x => x.ProductId).ObserveOn(NewThreadScheduler.Default)
			//let ProdVol = pgrp.Sum(x => x.Quantity) //.PublishLast(sum => sum)
			//from frgrp in pgrp.GroupBy(x => x.FillReason) 
			//let Vol = frgrp.Sum(x => x.Quantity)//.Publish().RefCount()
			from vol in pgrp.Sum(x => x.Quantity)
			//from count in pgrp.Count()
			//from frgrp in pgrp.GroupBy(x => x.FillReason)
			//from frvol in frgrp.Sum(x => x.Quantity)
			select new {
				Fills = pgrp.Buffer(window), //.GroupBy(x => x.FillReason),// .ToDictionary(x => x.Key , x => x.Tolist()),
				ProductId = pgrp.Key, 
				//Count = count, //pgrp.Count(),
				Vol = vol
				//FR = frgrp.Key,
				//FRVol = frvol
				//PVol = Vol.Zip(ProdVol, (frvol,vol) => (frvol / vol ) * 100)
				})
				//.SelectMany(p2grp => 
				//	from frgrp in p2grp.Fills.GroupBy(x => x.FillReason)
				//	select frgrp)
			
		);
	
	winthenGrpSum2.Dump("winthenGrpSum2");
	
	//winthenGrp.Select(x => x).Dump("winthenGrp");
	var winthenGrpSum = (from prgrp in winthenGrp
			let pgrp = prgrp.Replay().RefCount()
			//query.Select(x => x.Select(y => y.Quantity).Sum().Take(1));
			//let totq = fr.Select(x => x.Quantity).Sum().Take(1)
			//from ex in fr
			//let dVol = await pgrp.Sum(x => x.Quantity)
			//let ProdVol = pgrp.Sum(x => x.Quantity).PublishLast() 
			select new { 
				PGRP = pgrp, 
				ProductId = prgrp.Key, 
				Count = pgrp.Count(),
				Vol = pgrp.Sum(x => x.Quantity),
				FRS = (from frgrp in pgrp.GroupBy(x => x.FillReason) 
						//let frvol = frgrp.Sum(x => x.Quantity).PublishLast().RefCount()
						from pfrvol in frgrp.Sum(x => x.Quantity).Zip(pgrp.Sum(x => x.Quantity), (x,y) => new { x , y, PcVol = x/(double)y})
						select new { 
							//FRGRP = frgrp, 
							FR = frgrp.Key,
							//Count = frgrp.Count(),
							//PVol = Vol,
							//FRVol = frvol,
							PVol = pfrvol //pgrp.Sum(x => x.Quantity).Select(pvol => frvol / pvol)
							//PerFRVol = Vol.Zip(ProdVol, (frvol,vol) => (frvol / vol ) * 100).LastAsync()
						})
						//.ToList()
						//.ToDictionary(x => x.FR, x => new { Vol = x.FRVol, Perc = x.PVol })
			})
			;
			//.Select(x => x.Count.CombineLatest(x.(x,y)new { C = x.Count.Last() });
			//.SelectMany(x => x.
	
	winthenGrpSum.Dump("winthenGrpSum");		
	
	var grpThenWin = from trade in trades
				group trade by trade.ProductId into byProdId
				select byProdId;
				//from win in byProdId.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
				//select win; //.Publish().RefCount();
	
	//windows.Dump();
	grpThenWin = grpThenWin.Publish().RefCount();
	//grpThenWin.Dump("grpThenWin");
	
	var sum1 = from grp in grpThenWin
			from win in grp.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
	//query.Select(x => x.Select(y => y.Quantity).Sum().Take(1));
			//let totq = fr.Select(x => x.Quantity).Sum().Take(1)
			let winp = win.Publish().RefCount()//.ToEnumerable()//.ToList()
			let winTotal = winp.Select(x => x.Quantity).Sum()//.Take(1)
			select winp.ToList();
			
	//var sum2 = from tex in sum1
	//		group tex by tex.FillReason into FRGrps
	//				  select new { FRGrps };
//			select new { 
//				ProductId = winp, //.Select(x => x).ToList(), 
//				Count = win.Count(),
//				//Vol = win.Select(x => x.Quantity).Sum().Take(1),
//				winTotal,
//				FRB = from tex in winp 
//					  group tex by tex.FillReason into FRGrps
//					  select new { FRGrps }
//				};
	//sum1.Dump("sum1");		

	var query3 = 
		from window in windows //trades.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
		from product in window.GroupBy(x => x.ProductId)
		select product.Select(p => p.Quantity).Sum().Take(1);
		
	var query2 = 
		from window in windows //trades.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
		from product in window.GroupBy(x => x.ProductId)
		let ProdVol = product.Select(p => p.Quantity).Sum().Publish().RefCount()
		from frGrp in product.GroupBy(ex => ex.FillReason)
		select new { ProductId = frGrp.Select(x => x.ProductId).Take(1), 
		Vol = frGrp.Select(x => x.Quantity).Sum().Take(1), 
		ProdVol = ProdVol.TakeLast(1) };

	
	//sum1.Dump();
	//query3.Dump();
	//query2.Dump();
	//trades.Dump();

}