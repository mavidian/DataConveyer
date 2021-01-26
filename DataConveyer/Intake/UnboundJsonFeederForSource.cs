//UnboundJsonFeederForSource.cs
//
// Copyright © 2020-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Orchestrators;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;


namespace Mavidian.DataConveyer.Intake
{
   internal class UnboundJsonFeederForSource : LineFeederForSource
   {
      private readonly JsonTextReader _jsonReader;
      private int _clstrNo;  // relevant if RespectClusters is present; otherwise 0
      private bool _before1stClstr;

      // Settings to be determined by the ctor:
      private readonly bool _detectClusters; // if present, JSON arrays will denote record clustering on intake (any change in array nesting, will constitute new cluster); if absent (default), array nesting of JSON objects is ignored.

      private readonly List<JsonToken> _tokensThatAreValues; // JSON tokens that identify values to output (i.e. items of the records)

      internal UnboundJsonFeederForSource(TextReader reader, int sourceNo, string settings) : base(reader, sourceNo)
      {
         //Note that reader is passed to base class, even though it is not used there (all relevant methods are overridden, so null could've been passed instead).
         //This is because base.Dispose (called by Dispose in this class) disposes the reader (there is no public JsonReader.Dispose method).

         var settingDict = settings?.SplitPairsToListOfTuples()?.ToDictionary(t => t.Item1, t => t.Item2);

         //Settings:
         //    DetectClusters - if present, JSON arrays will denote record clustering on intake (any change in array nesting, will constitute new cluster); if absent (default), array nesting of JSON objects is ignored.

         _detectClusters = settingDict?.ContainsKey("DetectClusters") ?? false;

         // Syntax rules for unbound JSON:
         // 1. Each record is a (top-level) JSON object, i.e.  {..}.
         // 2. Collection of records is either a top-level array of objects: [{..},..,{..}] or subsequent objects: {..}{..} (not a true JSON, but commonly used).
         // 3. JSON object can be nested in arrays to support clusters (if DetectClusters is present); any change in array nesting constitutes a new cluster.
         // 4. A column name is the same as Json.NET Path property; no implied column names.
         _jsonReader = new JsonTextReader(reader)
         {
            SupportMultipleContent = true // allow mulitple JSON objects (as records) in addition to a single JSON array; similarly, multiple arrays are allowed
         };
         _clstrNo = 0;
         _before1stClstr = true;
         _tokensThatAreValues = new List<JsonToken>() {
                                       JsonToken.String,
                                       JsonToken.Integer,
                                       JsonToken.Float,
                                       JsonToken.Boolean,
                                       JsonToken.Null,
                                       JsonToken.Undefined  // absence of value, e.g. "MyUndefinedValue":, - technically invalid JSON, but Json.NET parses it as Undefined
                                    };
      }


      public override Tuple<ExternalLine, int> GetNextLine()
      {
         return SupplyNextXrecord()?.ToTuple(_sourceNo);
      }

      public override async Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         var line = await SupplyNextXrecordAsync();
         return line?.ToTuple(_sourceNo);
      }

      public override void Dispose()
      {
         _jsonReader.Close();
         ((IDisposable)_jsonReader).Dispose();
         base.Dispose();
      }


      /// <summary>
      /// Read enough _jsonReader to determine subsequent record.
      /// </summary>
      /// <returns>The next record read.</returns>
      private Xrecord SupplyNextXrecord()
      {
         bool newCluster = false;
         while (_jsonReader.Read())
         {
            //Note that newCluster will be set if array nesting level is in any way different from prior record
            switch (_jsonReader.TokenType)
            {
               case JsonToken.StartArray:
               case JsonToken.EndArray:
                  newCluster = true;
                  break;
               case JsonToken.StartObject:
                  if (_detectClusters && (newCluster || _before1stClstr)) _clstrNo++;
                  _before1stClstr = false;
                  return new Xrecord(GetRecordFromJsonObject().ToList(), _clstrNo);
               ////default:
               ////   throw new InvalidDataException($"Unexpected token '{_jsonReader.Value}' of type '{_jsonReader.TokenType}' encountered in JSON intake.");
            }

         }
         return null;  // end of data
      }


      /// <summary>
      /// Read enough _jsonReader to determine all values in current record.
      /// Returned keys reflect the relative path to JSON elements and uniquely define each value.
      /// </summary>
      /// <returns>Sequence of flattened key-value pairs obtained from JSON input.</returns>
      public IEnumerable<Tuple<string, object>> GetRecordFromJsonObject()
      {
         Debug.Assert(_jsonReader.TokenType == JsonToken.StartObject);
         var initialPath = _jsonReader.Path;  // to be removed from compound column name
         int nestingLevel = 0;
         while (_jsonReader.Read())
         {
            var tokenType = _jsonReader.TokenType;
            if (_tokensThatAreValues.Contains(_jsonReader.TokenType))
            {
               Debug.Assert(initialPath.Length == 0 || _jsonReader.Path.StartsWith(initialPath + "."));
               var charsToTrimFromPath = initialPath.Length == 0 ? 0 : initialPath.Length + 1;
               yield return Tuple.Create(_jsonReader.Path.Substring(charsToTrimFromPath), _jsonReader.Value);
            }
            else if (tokenType == JsonToken.StartObject) nestingLevel++;
            else if (tokenType == JsonToken.EndObject)
            {
               if (nestingLevel == 0) yield break;
               nestingLevel--;
            }
         }
         throw new InvalidDataException("Unexpected end of data encountered.");
      }


      /// <summary>
      /// Asynchronously read enough _jsonReader to determine subsequent record.
      /// </summary>
      /// <returns>A task with the next record read.</returns>
      private async Task<Xrecord> SupplyNextXrecordAsync()
      {
         while (await _jsonReader.ReadAsync())
         {
            if (_jsonReader.TokenType == JsonToken.StartObject)
            {
               return new Xrecord(await GetRecordFromJsonObjectAsync());
            }
         }
         return null;  // end of data
      }


      /// <summary>
      /// Asynchronously read enough _jsonReader to determine all values in current record.
      /// Returned keys reflect the relative path to JSON elements and uniquely define each value.
      /// </summary>
      /// <returns>A task with sequence of flattened key-value pairs obtained from JSON input.</returns>
      public async Task<List<Tuple<string, object>>> GetRecordFromJsonObjectAsync()
      {
         //TODO: use IAsyncEnumerable instead of Task<IEnumerable>
         Debug.Assert(_jsonReader.TokenType == JsonToken.StartObject);
         var initialPath = _jsonReader.Path;  // to be removed from compound column name
         int nestingLevel = 0;
         var seqToReturn = new List<Tuple<string, object>>();
         while (await _jsonReader.ReadAsync())
         {
            var tokenType = _jsonReader.TokenType;
            if (_tokensThatAreValues.Contains(_jsonReader.TokenType))
            {
               Debug.Assert(initialPath.Length == 0 || _jsonReader.Path.StartsWith(initialPath + "."));
               var charsToTrimFromPath = initialPath.Length == 0 ? 0 : initialPath.Length + 1;
               seqToReturn.Add(Tuple.Create(_jsonReader.Path.Substring(charsToTrimFromPath), _jsonReader.Value));
            }
            else if (tokenType == JsonToken.StartObject) nestingLevel++;
            else if (tokenType == JsonToken.EndObject)
            {
               if (nestingLevel == 0) return seqToReturn;
               nestingLevel--;
            }
         }
         throw new InvalidDataException("Unexpected end of data encountered.");
      }


      /// <summary>
      /// Consume the remainder of input stream, swallow its contents and the dispose the input
      /// </summary>
      public void ReadToEnd()
      {
         while (_jsonReader.Read()) { }
      }


      /// <summary>
      /// Asynchronously consume the remainder of input stream, swallow its contents and the dispose the input
      /// </summary>
      public async Task ReadToEndAsync()
      {
         while (await _jsonReader.ReadAsync()) { }
      }

   }
}
