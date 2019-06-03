//OutputProvider.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// Defines a set of functions that need to be supplied to the Output part of the EtlOrchestrator (Strategy Pattern).
   /// </summary>
   internal abstract class OutputProvider
   {
      protected readonly OrchestratorConfig _config;
      private ILineDispenser _outputFileWriter;

      private IReadOnlyList<string> _fieldsToUse;  //should only be written once ("popsicle")
      internal IReadOnlyList<string> FieldsToUse { get { return _fieldsToUse; } }

      private readonly bool _outputToWriter;  //true = write to output writer (a file or explicit writer); false = write to output consumer

      private class LineCounts  //container of written line counts
      {
         public LineCounts(int all, int extra) { All = all; Extra = extra; }
         public int All { get; set; }     // all lines, including leader/header/trailer
         public int Extra { get; set; }   // leader/header/trailer lines
      }

      private readonly ConcurrentDictionary<int, LineCounts> _targets;  //key = targetNo, value = counts of written lines (all lines and extra lines, i.e. leader/header/trailer)

      internal int WrittenLinesTotalCnt { get { return _targets.Values.Select(t => t.All).Sum(); } }     // all written lines, including leader/header/trailer lines
      internal int WrittenLinesExtraCnt { get { return _targets.Values.Select(t => t.Extra).Sum(); } }   // leader/header/trailer lines written
      internal int WrittenDataLinesCnt { get { return WrittenLinesTotalCnt - WrittenLinesExtraCnt;  } }  // data lines written (this excludes leader/header/trailer) 

      private int _lastTargetNo;

      private readonly SingleUseBool _atOutputStart;

      private IEnumerable<string> _leaderLines;
      private string _headerLine;  //expected to be set once known (~ReadyForOutput)

      internal virtual Func<X12Delimiters> X12Delimiters { get; }  //intended to be overridden only in X12OutputProvider

      private readonly IGlobalCache _globalCache;

      protected OutputProvider(OrchestratorConfig config, IGlobalCache globalCache)
      {
         _config = config;
         _globalCache = globalCache;

         if (config.OutputFields == null)
         {
            _fieldsToUse = null;  //this will be overwritten based on actual set of fields used (SetFieldsToUse method)
         }
         else  //Output fields specified in config, they will drive output
         {     //this list of field names will not be overwritten
            _fieldsToUse = config.OutputFields.ListOfSingleElements(0)?.ToList();
            Debug.Assert(_fieldsToUse.IsNonEmptyList());  //if specified, the list must be complete
            //TODO: Error message instead of the above Assert (part of "config scrubber")
         }

         _outputToWriter = _config.AsyncOutput ? _config.AsyncOutputConsumer == OrchestratorConfig.DefaultAsyncOutputConsumer && (_config.OutputWriters != null || _config.OutputFileNames != null)
                                               : _config.OutputConsumer == OrchestratorConfig.DefaultOutputConsumer && (_config.OutputWriters != null || _config.OutputFileNames != null);

         _initErrorOccurred = new Lazy<bool>(() => !InitOutput());

         _targets = new ConcurrentDictionary<int, LineCounts>();
         _lastTargetNo = 1;

         _atOutputStart = new SingleUseBool();

      } //ctor

      private readonly Lazy<bool> _initErrorOccurred;  //a placeholder to hold result of output initialization

      /// <summary>
      /// True denotes a problem during output initialization, e.g. a locked file
      /// Note that the actual initialization only occurs when this property is accessed for the first time.
      /// </summary>
      internal bool InitErrorOccurred
      {
         get { return _initErrorOccurred.Value; }
      }

      /// <summary>
      /// Performs one time initialization of the output target(s), e.g. open output file(s).
      /// This method is only called when the InitErrorOccurred property is accessed for the first time
      /// (which may not happen at all in case intake initialization fails).
      /// </summary>
      /// <returns>true if initialization successful; false if not, e.g. file not found</returns>
      private bool InitOutput()
      {
         // Note that in order for the exceptions to be caught in the try blocks below, CreateLineDispenser (or OutputInitializer as applicable) processing
         // must be eagerly evaluated (no deferred execution). 
         if (_outputToWriter)
         {
            try
            {
               //In case both OutputWriters and OutputFileNames are present, then OutputWriters wins
               _outputFileWriter = _config.OutputWriters == null ? LineDispenserCreator.CreateLineDispenser(_config.OutputFileNames.ToListOfStrings('|'), _config.OutputDataKind, _config.AsyncOutput, X12Delimiters == null ? null : new Lazy<string>(() => X12Delimiters().X12SegmentDelimiter), _config.XmlJsonOutputSettings)
                                                                 : LineDispenserCreator.CreateLineDispenser(_config.OutputWriters(), _config.OutputDataKind, _config.AsyncOutput, X12Delimiters == null ? null : new Lazy<string>(() => X12Delimiters().X12SegmentDelimiter), _config.XmlJsonOutputSettings);
               //Lazy here allows delimiters to be evaluated AFTER the first intake row (such as ISA segment), so that in absence of delimiters in config, they can be copied from intake
            }
            catch (Exception ex)
            {
               //something went wrong with output file(s) or text writer(s), there is not much we can do other than to report it
               var errMsg = _config.IntakeReaders == null ? $"Attempt to create output file(s) '{_config.OutputFileNames}' failed."
                                                          : "Attempt to open text writer(s) failed.";
               _config.Logger.LogFatal(errMsg, ex);
               return false;
            }
            return true;
         }
         else  //output to OutputConsumer or (OutputConsumerAsync) action
         {
            string errMsg;
            try
            {
               errMsg = _config.OutputInitializer(_globalCache);
            }
            catch (Exception ex)
            {
               _config.Logger.LogFatal("Output initializer threw exception.", ex);
               return false;
            }
            if (errMsg != null)
            {
               _config.Logger.LogFatal("Output initializer failed:\r\n" + errMsg);
               return false;
            }
            return true;
         }
      }


      /// <summary>
      /// Factory method that returns a concrete instance of the derived class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="x12Delimiters"></param>
      /// <returns></returns>
      internal static OutputProvider CreateProvider(OrchestratorConfig config, IGlobalCache globalCache, X12Delimiters x12Delimiters)
      {
         switch (config.OutputDataKind)
         {
            case KindOfTextData.Raw:
               return new RawOutputProvider(config, globalCache);
            case KindOfTextData.Keyword:
               return new KwOutputProvider(config, globalCache);
            case KindOfTextData.Delimited:
               return new DelimitedOutputProvider(config, globalCache);
            case KindOfTextData.Flat:
               return new FlatOutputProvider(config, globalCache);
            case KindOfTextData.Arbitrary:
               return new ArbitraryOutputProvider(config, globalCache);
            case KindOfTextData.X12:
               return new X12OutputProvider(config, globalCache, x12Delimiters);
            case KindOfTextData.XML:
               var allItemTypes = ItemType.Void | ItemType.Bool | ItemType.DateTime | ItemType.Decimal | ItemType.Int | ItemType.String;
               return new XrecordOutputProvider(allItemTypes, config, globalCache);
            case KindOfTextData.JSON:
               return new XrecordOutputProvider(ItemType.DateTime, config, globalCache);
            default:
               //TODO: Message - fatal error, undetermined type of output data
               return null;
         }
      }


      /// <summary>
      /// Helper method to provide a complete ("fixed") list of items based on FieldsToUse, including missing/insignificant items
      /// (if outputRecord contains no corresponding field/item, use VoidKeyValItem)
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      protected IEnumerable<IItem> ItemsFromFieldsToUse(KeyValRecord outputRecord)
      {
         foreach (var field in FieldsToUse)
         {
            if (outputRecord.Keys.Contains(field))
            {
               yield return outputRecord.GetItem(field);
            }
            else
            {
               yield return new VoidKeyValItem(field);
            }
         }
      }


#region Functions common to all providers

      /// <summary>
      /// Take a single output line (along with target number) and send it to the output
      /// </summary>
      /// <param name="tpl">Tuple containing the output line to send and the target number; null means end of data (must be the last call)</param>
      internal void SendLineToOutput(Tuple<ExternalLine, int> tpl)
      {
         var targetNo = tpl?.Item2 ?? _lastTargetNo;
         _lastTargetNo = targetNo;

         var countsSoFar = _targets.GetOrAdd(targetNo, k => new LineCounts(0, 0));

         if (countsSoFar.All == 0)  //the first line for the target
         {
            var atOutputStart = _atOutputStart.FirstUse;   //true at the very start of output processing, false afterwards
            if (atOutputStart || _config.RepeatLeaders)
            {
               if (_leaderLines != null) SendExtraLines(_leaderLines.Select(l => l.ToExternalTuple(targetNo)));
            }
            if (atOutputStart || _config.RepeatOutputHeaders)
            {
               if (_headerLine != null)
               {
                  SendNextLine(_headerLine.ToExternalTuple(targetNo));
                  countsSoFar.Extra++;  // increment WrittenLinesExtraCnt count
               }
            }
         }

         if (tpl == null) SendTrailers();

         SendNextLine(tpl);
      }


      /// <summary>
      /// Take a single output line (along with target number) and send it to the output asynchronously
      /// </summary>
      /// <param name="tpl">Tuple containing the output line to send and the target number; null means end of data (must be the last call)</param>
      internal async Task SendLineToOutputAsync(Tuple<ExternalLine, int> tpl)
      {
         var targetNo = tpl?.Item2 ?? _lastTargetNo;
         _lastTargetNo = targetNo;

         var countsSoFar = _targets.GetOrAdd(targetNo, k => new LineCounts(0, 0));

         if (countsSoFar.All == 0)  //the first line for the target
         {
            var atOutputStart = _atOutputStart.FirstUse;   //true at the very start of output processing, false afterwards
            if (atOutputStart || _config.RepeatLeaders)
            {
               if (_leaderLines != null) await SendExtraLinesAsync(_leaderLines.Select(l => l.ToExternalTuple(targetNo)));
            }
            if (atOutputStart || _config.RepeatOutputHeaders)
            {
               if (_headerLine != null)
               {
                  await SendNextLineAsync(_headerLine.ToExternalTuple(targetNo));
                  countsSoFar.Extra++;  // increment WrittenLinesExtraCnt count
               }
            }
         }

         if (tpl == null) await SendTrailersAsync();

         await SendNextLineAsync(tpl);
      }


      /// <summary>
      /// Dispose output, such as the writer writing to a file
      /// </summary>
      internal void DisposeOutput()
      {
         //This method gets called from EtlOrchestrator in case of cancellation
         if (_outputToWriter)
         {
            _outputFileWriter?.Dispose();  //note that writer can be null if ConfigRejected
         }
         else  //output to OutputConsumer (orOutputConsumerAsync) function
         {
            try { _config.OutputDisposer(_globalCache); }
            catch (Exception ex)
            { //errors during disposal are logged, but otherwise ignored
               _config.Logger.LogError("Error occurred during output disposal (OutputDisposer function).", ex);
            }
         }
      }


      //Note that there is no equivalent to ClusterMarker function from IntakeProvider; records from next cluster simply follow (are joined to) the records from prior cluster


      /// <summary>
      /// Assign FieldsToUse (can only be assigned once, "popsicle" immutability)
      /// </summary>
      /// <param name="fieldsToUse"></param>
      internal virtual void SetFieldsToUse(IReadOnlyList<string> fieldsToUse)
      {
         if (_fieldsToUse == null) _fieldsToUse = fieldsToUse;  //no action in case of subsequent attempt to set, original value
      }                                     


      /// <summary>
      /// Accept the header line and determine leader lines to be sent before writing first data line to the target(s).
      /// </summary>
      /// <param name="headerLine"></param>
      internal void AssignLeadersAndHeader(string headerLine)
      {
         //This method is expected to be called once the header row contents is known (~ReadyForOutput).
         //Note that there is no SendLeadersAndHeaderAsync (no asynchronous or long running operation here).
         _leaderLines = _config.LeaderContents?.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
         _headerLine = headerLine;
      }

#endregion Functions common to all providers


#region Functions to be provided (overridden) by specific provider

      /// <summary>
      /// A function that takes a set of tokens and returns the output line
      /// </summary>
      internal abstract Func<IEnumerable<string>, ExternalLine> TokenJoiner { get; }

      /// <summary>
      /// A matching function that takes an item (KeyValItem) along with field# and returns a token (string)
      /// </summary>
      internal abstract Func<IItem, int, string> TokenFromItem { get; }

      /// <summary>
      /// Return the actual set of fields that need to be included in output 
      /// </summary>
      /// <param name="outputRecord">Current output record</param>
      /// <returns></returns>
      internal abstract IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord);

      /// <summary>
      /// A function that takes a field name along with a field# and returns a token (string) as it needs to appear in the header row
      /// The returned token is expected to be used to construct header row if HeadersInFirstOutputRow=true (for delimited or flat output)
      /// </summary>
      internal abstract Func<string, int, string> TokenForHeaderRow { get; }

      /// <summary>
      /// A function that takes an item (KeyValItem) and returns a tuple (key value pair provided to Xrecord dispensers, such as XML or JSON).
      /// This function is used in XrecordOutputProvider instead of TokenFromItem and TokenJoiner.
      /// </summary>
      internal abstract Func<IItem, Tuple<string, object>> ExtItemFromItem { get; }

#endregion Functions to be provided (overridden) by specific provider


#region Private functions

      /// <summary>
      /// Send a single text line (along with target number) to either text file (dispenser) or output consumer
      /// </summary>
      /// <param name="tplToSend"></param>
      private void SendNextLine(Tuple<ExternalLine, int> tplToSend)
      {
         if (_outputToWriter)
         {
            _outputFileWriter.SendNextLine(tplToSend);
         }
         else
         {
            _config.OutputConsumer(tplToSend, _globalCache);
         }

         if (tplToSend != null) _targets[tplToSend.Item2].All++;  // increment WrittenLinesTotalCnt count
      }



      /// <summary>
      /// Asynchronously send a single text line (along with target number) to either text file (dispenser) or output consumer
      /// </summary>
      /// <param name="tplToSend"></param>
      /// <returns></returns>
      private async Task SendNextLineAsync(Tuple<ExternalLine, int> tplToSend)
      {
         if (_outputToWriter)
         {
            await _outputFileWriter.SendNextLineAsync(tplToSend);
         }
         else
         {
            await _config.AsyncOutputConsumer(tplToSend, _globalCache);
         }

         if (tplToSend != null) _targets[tplToSend.Item2].All++;  // increment WrittenLinesTotalCnt count
      }


      /// <summary>
      /// Send multiple text lines (along with target numbers), such as leader/trailer lines
      /// </summary>
      /// <param name="tplsToSend"></param>
      private void SendExtraLines(IEnumerable<Tuple<ExternalLine, int>> tplsToSend)
      {
         //note that external lines are always of text type (Xtext)
         foreach (var line in tplsToSend)
         {
            Debug.Assert(line.Item1.Type == ExternalLineType.Xtext);
            SendNextLine(line);
            _targets[line.Item2].Extra++;  // increment WrittenLinesExtraCnt count

         }
      }


      /// <summary>
      /// Asynchronously send multiple text lines (along with target numbers), such as leader/trailer lines
      /// </summary>
      /// <param name="tplsToSend"></param>
      private async Task SendExtraLinesAsync(IEnumerable<Tuple<ExternalLine, int>> tplsToSend)
      {
         //note that external lines are always of text type (Xtext)
         foreach (var line in tplsToSend)
         {
            Debug.Assert(line.Item1.Type == ExternalLineType.Xtext);
            await SendNextLineAsync(line);
            _targets[line.Item2].Extra++;  // increment WrittenLinesExtraCnt count
         }
      }


      /// <summary>
      /// Send trailer lines (if any) to appropriate targets
      /// </summary>
      private void SendTrailers()
      {
         var trailerLines = _config.TrailerContents?.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
         if (trailerLines == null) return;

         //Trailer lines are sent to either all targets or only to target that received the last line 
         if (_config.RepeatTrailers)
         {  // send trailers to all targets
            _targets.Keys.ToList().ForEach(t => SendExtraLines(trailerLines.Select(l => l.ToExternalTuple(t))));
         }
         else
         {  // send trailers only to target that received the last line (or first target if no lines sent at all)
            SendExtraLines(trailerLines.Select(l => l.ToExternalTuple(_lastTargetNo)));
         }
      }


      /// <summary>
      /// Asynchronously send trailer lines (if any) to appropriate targets
      /// </summary>
      private async Task SendTrailersAsync()
      {
         var trailerLines = _config.TrailerContents?.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
         if (trailerLines == null) return;

         //Trailer lines are sent to either all targets or only to target that received the last line 
         if (_config.RepeatTrailers)
         {  // send trailers to all targets
            _targets.Keys.ToList().ForEach(async t => await SendExtraLinesAsync(trailerLines.Select(l => l.ToExternalTuple(t))));
         }
         else
         {  // send trailers only to target that received the last line (or first target if no lines sent at all)
            await SendExtraLinesAsync(trailerLines.Select(l => l.ToExternalTuple(_lastTargetNo)));
         }
      }

#endregion Private functions

   }

}