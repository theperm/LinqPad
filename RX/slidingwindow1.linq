<Query Kind="Statements">
  <NuGetReference>Rx-Main</NuGetReference>
  <Namespace>System.Reactive.Linq</Namespace>
</Query>

// generate some random prices
var random = new Random();

var price = Observable.Generate(
0,
x => true,
x => x + 1, // - 0.5 + random.NextDouble(), 
x => x,
x => TimeSpan.FromSeconds(0.5));

var query = 
 from window in price.Window(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1))
 //from average in window.Average()
 select window;
 //from x in window.
 //select x;

query.Dump("query");

var grps = from win in query
			group win by win
price.Dump("price");