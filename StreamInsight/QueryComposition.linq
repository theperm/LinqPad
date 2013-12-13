<Query Kind="Program">
  <Connection>
    <ID>04e9134a-210f-4266-9de5-607b0fea07c9</ID>
    <Persist>true</Persist>
    <Driver Assembly="StreamInsightLinqPad" PublicKeyToken="3d3a4b0768c9178e">StreamInsightLinqPad.StaticDriver</Driver>
    <CustomAssemblyPath>StreamInsightLinqPad.Samples.dll</CustomAssemblyPath>
    <CustomTypeName>StreamInsightLinqPad.Samples.StreamInsightContext</CustomTypeName>
    <DriverData>
      <ContextKind>StreamInsight</ContextKind>
      <RequiredVersion>21.0.0.0</RequiredVersion>
    </DriverData>
  </Connection>
</Query>

/// <summary>
/// Simulating an event source payload.
/// </summary>
public class SourcePayload
{
  public DateTime Timestamp { get; set; }
  public double Value { get; set; }

  public SourcePayload()
  {
      // making up payload values
      this.Timestamp = DateTime.Now;
      this.Value = DateTime.Now.Second;
  }
}

static void Main(string[] args)
{
  using (Server server = Server.Create("SI_21_Instance1"))
  {
      try
      {
          Application myApp = server.CreateApplication("MyApp");

          #region Definitions

          // define a source: timer-based data generator, as a function of the time interval
          var generator = myApp.DefineObservable(
              (TimeSpan t) => Observable.Interval(t).Select(e => new SourcePayload()));

          // define a subject, which is an observable sequence as well as an observer.
          // We need a subject to be able to de-couple the queries from the source
          // and have the source be instantiated only once.
          // Subjects are deployed by definition.
          var subject = myApp.CreateSubject("Subject", () => new Subject<SourcePayload>());

          // define and deploy a sink: console output
          // name it to be able to retrieve it from a separate client.
          var console = myApp.DefineObserver(
              (string label) => Observer.Create<double>(e => Console.WriteLine(label + ": " + e)))
              .Deploy("ConsoleSink");

          // Define and deploy a query template as an IQStreamable, as a function
          // of an input stream and a window length.
          // note that the deploy step wouldn't be necessary for a purely embedded server,
          // since we could just refer to the object 'avg' further down.
          var avg = myApp
              .DefineStreamable((IQStreamable<SourcePayload> s, TimeSpan w) =>
                  from win in s.TumblingWindow(w)
                  select win.Avg(e => e.Value))
              .Deploy("AverageQuery");

          #endregion

          #region First query

          // promote the subject's sequence to a temporal stream
          var stream = subject.ToPointStreamable(
              e => PointEvent.CreateInsert<SourcePayload>(e.Timestamp, e),
              AdvanceTimeSettings.StrictlyIncreasingStartTime);

          // apply the average query template to the input stream
          var longAverages = avg(stream, TimeSpan.FromSeconds(5));

          // the 'slow' query: binding the query to the sink, and the source to the subject
          var slowQuery = longAverages
              .Bind(console("5sec average"))
              .With(generator(TimeSpan.FromMilliseconds(300)).Bind(subject));

          Console.WriteLine("Long aggregate query starting.");

          // run the slow query
          slowQuery.Run("StandardProcess");

          #endregion

          #region Second query
          
          // simulate the addition of a 'fast' query from a separate server connection,
          // by retrieving the aggregation query template
          // (instead of simply using the 'avg' object)
          var averageQuery = myApp.GetStreamable<IQStreamable<SourcePayload>, TimeSpan, double>("AverageQuery");

          // retrieve the input sequence as a subject
          var inputSequence = myApp.GetSubject<SourcePayload, SourcePayload>("Subject");

          // retrieve the registered sink
          var sink = myApp.GetObserver<string, double>("ConsoleSink");

          // turn the sequence into a temporal stream
          var stream2 = inputSequence.ToPointStreamable(
              e => PointEvent.CreateInsert<SourcePayload>(e.Timestamp, e),
              AdvanceTimeSettings.StrictlyIncreasingStartTime);

          // apply the query, now with a different window length
          var shortAverages = averageQuery(stream2, TimeSpan.FromSeconds(1));

          // bind new sink to query
          var fastQuery = shortAverages
              .Bind(sink("1sec average"));
          
          #endregion

          Console.WriteLine("Press S to toggle the short aggregate query. Press Q to quit.");

          var key = Console.ReadLine().ToUpper().First();

          IDisposable fastProcess = null;

          while (key != 'Q')
          {
              key = Console.ReadLine().ToUpper().First();

              if (key == 'S')
              {
                  if (fastProcess == null)
                  {
                      fastProcess = fastQuery.Run("FastProcess");
                  }
                  else
                  {
                      fastProcess.Dispose();
                      fastProcess = null;
                  }
              }
          }
      }
      catch (Exception e)
      {
          Console.WriteLine(e.ToString());
      }
  }
}