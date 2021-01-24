//IntakeProvider.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// Defines a set of functions that need to be supplied to the Intake part of the EtlOrchestrator (Strategy Pattern)
   /// </summary>
   internal abstract class IntakeProvider
   {
      protected readonly OrchestratorConfig _config;
      protected readonly TypeDefinitions _typeDefinitions;

      private ILineFeeder _intakeReader;  //either from file (config.InputFileName) or explicit reader (config.IntakeReader)

      protected bool _newFieldsAllowed;  //true means FieldsInUse can grow (should only change from true to false, i.e. "popsicle")

      private readonly List<string> _fieldsInUse;
      //TODO: Consider a more efficient way of storing a distinct, ordered list
      //      (note that for KW data IncludeField (and hence Contains) is called for every field in every record)

      /// <summary>
      /// Contains a sequence of unique fields names encountered when processing intake
      /// </summary>
      internal IReadOnlyList<string> FieldsInUse { get { return _fieldsInUse.AsReadOnly(); } }

      internal int _lineCnt;  //number of lines (records) read (mutating)

      private readonly bool _intakeFromReader;  //true = read from intake reader (a file or explicit reader); false = read from intake supplier

      protected readonly Func<int, string> DefaultFldName;  //a function to derive a default field name (key) from a given field seq# (e.g. 0 -> Fld001, 1 -> Fld002, etc.) - special formula may be provided from derived class ctor
      protected readonly List<string> FieldsNamesFromConfig;     //a list of field names if defined in config.InputFields

      private readonly object _locker = new object();

      private readonly IGlobalCache _globalCache;

      protected IntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions, Func<int, string> defaultFldName = null)
      {
         _config = config;
         _globalCache = globalCache;
         _typeDefinitions = typeDefinitions;
         _fieldsInUse = new List<string>();
         _newFieldsAllowed = true;

         DefaultFldName = defaultFldName ?? (sNo => string.Format("Fld{0:000}", sNo + 1));  //unless special formula from derived class, use Fld001, Fld002, ...

         //Preload field names from config, if any
         FieldsNamesFromConfig = config.InputFields?.ListOfSingleElements(0)?.ToList();
         if (!FieldsNamesFromConfig.IsEmptyList()) IncludeFieldsEnMasse(FieldsNamesFromConfig);

         _intakeFromReader = _config.AsyncIntake ? _config.AsyncIntakeSupplier == OrchestratorConfig.DefaultAsyncIntakeSupplier && (_config.IntakeReaders != null || _config.InputFileNames != null)
                                                 : _config.IntakeSupplier == OrchestratorConfig.DefaultIntakeSupplier && (_config.IntakeReaders != null || _config.InputFileNames != null);

         _initErrorOccurred = new Lazy<bool>(() => !InitIntake());

         _lineCnt = 0;

      } //ctor


      private readonly Lazy<bool> _initErrorOccurred;  //a placeholder to hold result of intake initialization

      /// <summary>
      /// True denotes a problem during intake initialization, e.g. input file not found.
      /// Note that the actual initialization only occurs when this property is accessed for the first time.
      /// </summary>
      internal bool InitErrorOccurred
      {
         get { return _initErrorOccurred.Value; }
      }

      /// <summary>
      /// Performs one time initialization of the intake source(s), e.g. open input file(s).
      /// This method is only called when the InitErrorOccurred property is accessed for the first time.
      /// </summary>
      /// <returns>true if initialization successful; false if not, e.g. file not found</returns>
      private bool InitIntake()
      {
         var skipRepeatedHeader = _config.InputDataKind.CanHaveHeaderRow() && _config.HeadersInFirstInputRow && _config.InputHeadersRepeated;
         var x12SegmentDelimiter = _config.InputDataKind == KindOfTextData.X12 ? _config.DefaultX12SegmentDelimiter ?? "~" : null;  //note that default delimiter (~) is unlikely to be used as it get overwritten by pos 106 of ISA segment

         // Note that in order for the exceptions to be caught in the try blocks below, CreateLineFeeder (or IntakeInitializer as applicable) processing
         // must be eagerly evaluated (no deferred execution). 
         if (_intakeFromReader)
         {
            try
            {
               //In case both IntakeReaders and InputFileNames are present, then IntakeReaders wins
               _intakeReader = _config.IntakeReaders == null ? LineFeederCreator.CreateLineFeeder(_config.InputFileNames.ToListOfStrings('|'), _config.InputDataKind, _config.AsyncIntake, skipRepeatedHeader, x12SegmentDelimiter, _config.XmlJsonIntakeSettings)
                                                             : LineFeederCreator.CreateLineFeeder(_config.IntakeReaders(), _config.InputDataKind, _config.AsyncIntake, skipRepeatedHeader, x12SegmentDelimiter, _config.XmlJsonIntakeSettings);
            }
            catch (Exception ex)
            {
               //something went wrong with input file(s) or text reader(s), there is not much we can do other than to report it
               var errMsg = _config.IntakeReaders == null ? $"Attempt to access input file(s) '{_config.InputFileNames}' failed."
                                                          : "Attempt to open text reader(s) failed.";
               _config.Logger.LogFatal(errMsg, ex);
               return false;
            }
            return true;
         }
         else  //intake from IntakeSupplier (or IntakeSupplierAsync) function
         {
            string errMsg;
            try
            {
               errMsg = _config.IntakeInitializer(_globalCache);
            }
            catch (Exception ex)
            {
               _config.Logger.LogFatal("Intake initializer threw exception.", ex);
               return false;
            }
            if (errMsg != null)
            {
               _config.Logger.LogFatal("Intake initializer failed:\r\n" + errMsg);
               return false;
            }
            return true;
         }
      }


      /// <summary>
      /// Add a new field to FieldsInUse, unless already there
      /// </summary>
      /// <param name="key"></param>
      /// <returns>true if the field was added to FieldsInUse; false if not because it was already there</returns>
      protected bool IncludeField(string key)
      {
         if (_fieldsInUse.Contains(key)) return false;
         lock(_locker)
         {
            if (_fieldsInUse.Contains(key)) return false;
            _fieldsInUse.Add(key);
         }
         return true;
      }

      /// <summary>
      /// Adds series of fields to FieldsInUse (intended to be called upon processing of config data or the first row in delimited or flat data).
      /// Any missing field names will be substituted with default field names.
      /// </summary>
      /// <param name="fieldNames">Sequence of field names.</param>
      /// <returns>Number of fields added (the same as size of FieldNames)</returns>
      internal int IncludeFieldsEnMasse(IEnumerable<string> fieldNames)
      {
         Debug.Assert(_newFieldsAllowed);  //this method should only be called when processing config or header row

         int sNo = FieldsInUse.Count;

         //Even though the code below allows combining field names from both sources (config & header row), the resulting merged set would be too
         // confusing.  So instead of merging, we simply ignore the 1st row in case names have been provided in config.
         if (sNo > 0) return 0;

         if (fieldNames == null) fieldNames = Enumerable.Empty<string>();

         //Sum below causes immediate execution, so it's OK to use sNo closure
         return fieldNames.Select(k =>
                                        {
                                           //here, the field name can be quoted and/or contain leading/trailing whitespace
                                           k = k.UnquoteIfNeeded(_config.RetainQuotes).TrimIfNeeded(_config.TrimInputValues);
                                           if (string.IsNullOrWhiteSpace(k) || !IncludeField(k))  //only attempt to add a passed key that is non-empty
                                           {  //if empty or dup key, then use default field name
                                              int fNo = sNo;
                                              while (!IncludeField(DefaultFldName(fNo))) { fNo++; }  //names are never duplicated
                                           }
                                           sNo++;
                                           return true;
                                        }).Sum(yn => yn ? 1 : 0);
      }


      /// <summary>
      /// Set a flag to allow additions of new fields on the fly or not; intended to be called before processing of the 1st data row (e.g. after processing the header row)
      /// </summary>
      internal void HeaderRowIsComplete()
      {
         _newFieldsAllowed = _config.InputDataKind.OnTheFlyInputFieldsAreAlwasyAllowed() ? true : _config.InputDataKind.OnTheFlyInputFieldsCanBeAllowed() ? _config.AllowOnTheFlyInputFields : false;
      }


      /// <summary>
      /// Factory method that returns a concrete instance of the derived class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      /// <param name="x12DelimitersForOutput"></param>
      /// <returns></returns>
      internal static IntakeProvider CreateProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions, X12Delimiters x12DelimitersForOutput)
      {
         switch(config.InputDataKind)
         {
            case KindOfTextData.Raw:
               return new RawIntakeProvider(config, globalCache, typeDefinitions);
            case KindOfTextData.Keyword:
               return new KwIntakeProvider(config, globalCache, typeDefinitions);
            case KindOfTextData.Delimited:
               return new DelimitedIntakeProvider(config, globalCache, typeDefinitions);
            case KindOfTextData.Flat:
               return new FlatIntakeProvider(config, globalCache, typeDefinitions);
            case KindOfTextData.Arbitrary:
               return new ArbitraryIntakeProvider(config, globalCache, typeDefinitions);
            case KindOfTextData.X12:
               return new X12IntakeProvider(config, globalCache, typeDefinitions, x12DelimitersForOutput);
            case KindOfTextData.XML:
            case KindOfTextData.JSON:
            case KindOfTextData.UnboundJSON:
               return new XrecordIntakeProvider(config, globalCache, typeDefinitions);
            default:
               //TODO: Message - fatal error, undetermined type of intake data
               return null;
         }
      }


      /// <summary>
      /// Helper method to create a new item and also update a list of field names (FieldsInUse)
      /// </summary>
      /// <param name="key"></param>
      /// <param name="val"></param>
      /// <param name="typeDefinitons"></param>
      /// <returns>The item just created or null if item excluded (this happens in case key was new and new fields disallowed).</returns>
      protected IItem CreateItemAndMarkField(string key, string val, TypeDefinitions typeDefinitons)
      {
         //side-effect, i.e. update FieldsInUse
         //update only if key not yet in the list
         if (_newFieldsAllowed)
         {
            IncludeField(key);
            return KeyValItem.CreateItem(key, val, typeDefinitons);
         }
         else  //adding new fields to FieldsInUse is disallowed
         {
            if (FieldsInUse.Contains(key)) return KeyValItem.CreateItem(key, val, typeDefinitons);
            return null;  //item excluded
         }
      }


      /// <summary>
      /// Helper method to create a new item and also update a list of field names (FieldsInUse).
      /// This overload is intended for use when value is alredy of the intended type (e.g. in case of JSON).
      /// </summary>
      /// <param name="key"></param>
      /// <param name="val"></param>
      /// <param name="typeDefinitons"></param>
      /// <returns>The item just created or null if item excluded (this happens in case key was new and new fields disallowed).</returns>
      protected IItem CreateItemAndMarkField(string key, object val, TypeDefinitions typeDefinitons)
      {
         //side-effect, i.e. update FieldsInUse
         //update only if key not yet in the list
         if (_newFieldsAllowed)
         {
            IncludeField(key);
            return KeyValItem.CreateItem(key, val, typeDefinitons);
         }
         else  //adding new fields to FieldsInUse is disallowed
         {
            if (FieldsInUse.Contains(key)) return KeyValItem.CreateItem(key, val, typeDefinitons);
            return null;  //item excluded
         }
      }


      /// <summary>
      /// Helper method intended for use by ItemFromToken functions in Delimited and Flat intake providers.
      /// Takes a token (identified by text tokenizer that returns a sole value, e.g. delimited or flat file), a field sequence number (0 based)...
      /// and returns the KeyValItem object derived from this token.
      /// </summary>
      /// <param name="token">Token, a single string value</param>
      /// <param name="fldNo">Field sequence number (0 based)</param>
      /// <param name="typeDefinitions"></param>
      /// <returns>Constructed item or null if item was excluded.</returns>
      protected IItem ItemFromTextToken(string token, int fldNo, TypeDefinitions typeDefinitions)
      {
         return CreateItemAndMarkField(GetFieldName(fldNo),
                                        token.UnquoteIfNeeded(_config.RetainQuotes).TrimIfNeeded(_config.TrimInputValues),
                                        typeDefinitions);
      }


      /// <summary>
      /// Creates a KeyValItem for a given token obtained from TokenizeKwLine function.
      /// </summary>
      /// <param name="token">Token as returned by the TokenizeKwLine function.</param>
      /// <param name="typeDefinitons"></param>
      /// <returns>Constructed item or null if item was excluded.</returns>
      protected IItem ItemFromKwToken(string token, TypeDefinitions typeDefinitons)
      {

         //This "token to item" function works with tokens in Key=Value, where Value can have surrounding commas and/or whitespace
         // it also considers tokens that are not necessarily in Key=Value format, some pieces may be missing
         string key, val;
         int keyEnd = token.IndexOf('=');
         if (keyEnd < 0)
         {
            //no = in token means value is null; check if Key is also null
            key = string.IsNullOrWhiteSpace(token) ? null : token;
            val = null;
         }
         else
         {
            key = token.Substring(0, keyEnd);
            val = token.Substring(keyEnd + 1).TrimInFrontOfQuote().UnquoteIfNeeded(_config.RetainQuotes).TrimIfNeeded(_config.TrimInputValues);
         }

         if (key == null) return null;  //exclude completely empty items (both key and value are null, e.g. 2 consecutive commas in KW data)

         if (_config.InputKeyPrefix == null)
         {
            return CreateItemAndMarkField(key, val, typeDefinitons);
         }

         //Trim prefix from key
         if (key.StartsWith(_config.InputKeyPrefix))
         {
            return CreateItemAndMarkField(key.Substring(_config.InputKeyPrefix.Length), val, typeDefinitons);
         }

         //Mismatched prefix
         if (_config.ExcludeItemsMissingPrefix)
         {
            return null;  //item excluded
         }

         //here, key doesn't match prefix, but config says we need to include the item anyway (use the entire key)
         return CreateItemAndMarkField(key, val, typeDefinitons);

      }  //ItemFromKwToken


      /// <summary>
      /// Determine the name (key) of a given field
      /// </summary>
      /// <param name="fldNo">Field sequence number (0 based)</param>
      /// <returns>Name defined in the header row if present; otherwise "Fldxxx" where xxx is the seq# (1 based!)</returns>
      private string GetFieldName(int fldNo)
      {
         var fldsInUse = new List<string>(FieldsInUse);  //local copy to prevent updates to FieldsInUse (which may be disallowed based on newFieldsAllowed (AllowOnTheFlyInputFields) setting)
         ////while (fldNo >= fldsInUse.Count)  //note that this should only be executed once (i.e. fldNo should be no higher than currFldCnt + 1)
         string nameToReturn;
         //verify if the field is already among fields in use
         if (fldNo < fldsInUse.Count)
         {
            nameToReturn = fldsInUse[fldNo];
         }
         else  //not in FieldsInUse, determine the "next available" default name
         {
            nameToReturn = DefaultFldName(fldNo);
            int fNo = fldNo;
            while (fldsInUse.Contains(nameToReturn))  //keep trying until unique field name found
            {
               fNo++;
               nameToReturn = DefaultFldName(fNo);
            }
         }

         //Note that this is not thread safe, as FieldsInUse will not get updated with nameToReturn until later, specifically IncludeField call
         //  In the meantime, another thread can calculate the same default nameToReturn.
         //However, newFieldsAllowed (AllowOnTheFlyInputFields) should only be true for intake process, which is single threaded.
         return nameToReturn;
      }


#region Functions common to all providers

      /// <summary>
      /// Read a single line from intake. In case of X12, the line means segment (so, CR/LF is not (necessarily) a delimiter).
      /// </summary>
      /// <returns>A tuple consisting of the line sequence number (1-based), source number (1-based) and the line read from intake; or null as EOD mark</returns>
      internal Tuple<int, int, ExternalLine> GetLineFromIntake()
      {
         // linePlus is a Tuple<ExternalLine,int>: Item1 = line, Item2 = source number
         // End-of-data indicators: * non-null linePlus.Item1 means data
         //                         * null linePlus.Item1 means end-of-data for a single source (only if asynchronous)
         //                         * null linePlus means end-of-data for all sources
         Tuple<ExternalLine, int> linePlus;
         do //note that this loop is for consistency with async intake from files where tuple with end-of-data mark denotes end of each source (but not the entire intake) 
         {  // (even though in case of synchronous intake, tuples with null contents should not be received from LineFeeder or IntakeSupplier).
            //IOW, this loop should always execute a single iteration.
            linePlus = _intakeFromReader ? _intakeReader.GetNextLine() : _config.IntakeSupplier(_globalCache);
            Debug.Assert(linePlus == null || linePlus.Item1 != null);
         }
         while (linePlus != null && linePlus.Item1 == null);  // ignore tuples with null lines (end-of-data marks for individual sources)

         if (linePlus == null) return null;  // EOD mark

         var lineCnt = Interlocked.Increment(ref _lineCnt);  // adjustment for header row(s) may be needed

         return Tuple.Create(lineCnt, linePlus.Item2, linePlus.Item1);
      }


      /// <summary>
      /// Asynchronously read a single line from intake
      /// </summary>
      /// <returns>A tuple consisting of the line sequence number (1-based), source number (1-based) and the line read from intake; or null as EOD mark</returns>
      internal async Task<Tuple<int, int, ExternalLine>> GetLineFromIntakeAsync()
      {
         // TBD: Intake is from either text file(s) or IntakeSupplier function
         // linePlus is a Tuple<ExternalLine,int>: Item1 = line, Item2 = source number
         // End-of-data indicators: * non-null linePlus.Item1 means data
         //                         * null linePlus.Item1 means end-of-data for a single source (only if asynchronous)
         //                         * null linePlus means end-of-data for all sources
         Tuple<ExternalLine, int> linePlus;
         do
         {
            linePlus = _intakeFromReader ? await _intakeReader.GetNextLineAsync() : await _config.AsyncIntakeSupplier(null);
         }
         while (linePlus != null && linePlus.Item1 == null);  // ignore tuples with null lines (which in case of async _intakeFromFile denote end of each file)

         if (linePlus == null) return null;  // EOD mark

         var lineCnt = Interlocked.Increment(ref _lineCnt);  // adjustment for header row(s) may be needed

         return Tuple.Create(lineCnt, linePlus.Item2, linePlus.Item1);
      }


      /// <summary>
      /// Dispose the intake supplier, such as reader reading from a file
      /// </summary>
      internal void DisposeIntake()
      {
         //This method gets called from EtlOrchestrator when intake is done for any reason incl. cancellation
         if (_intakeFromReader)
         {
            _intakeReader?.Dispose(); //note that reader can be null if ConfigRejected
         }
         else  //intake from IntakeSupplier (or IntakeSupplierAsync) function
         {
            try { _config.IntakeDisposer(_globalCache); }
            catch (Exception ex)
            { //errors during disposal are logged, but otherwise ignored
               _config.Logger.LogError("Error occurred during intake disposal (IntakeDisposer function).", ex);
            }
         }
      }

#endregion Functions common to all providers


#region Functions to be provided (overridden) by specific provider

      /// <summary>
      /// A function that takes input line (string) and returns set of tokens
      /// </summary>
      internal abstract Func<ExternalLine, IEnumerable<string>> FieldTokenizer { get; }

      /// <summary>
      /// A matching function that takes a token (string) along with field# and returns an item (KeyValItem)
      /// </summary>
      internal abstract Func<string, int, IItem> ItemFromToken { get; }

      /// <summary>
      /// A function that takes a tuple (key value pair produced by Xrecord feeders, such as XML or JSON) and returns an item (KeyValItem).
      /// This function is used in XrecordIntakeProvider instead of FieldTokenizer and ItemFromToken.
      /// </summary>
      internal abstract Func<Tuple<string,object>, IItem> ItemFromExtItem { get; }

#endregion Functions to be provided (overridden) by specific provider

   }
}