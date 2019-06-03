//ArbitraryOutputProvider.cs
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
using Mavidian.DataConveyer.Intake;        //to "borrow" TrimIfNeeded extension method that resides in IntakeHelpers
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// A set of functions specific to Arbitrary data to be supplied to Output (Strategy Pattern)
   /// </summary>
   internal class ArbitraryOutputProvider : OutputProvider
   {
      private const string _magicToken = "!#`'`\xd1\x16\xf0";  //unique string used for substitutions (taking chances it will not appear in ArbitraryOutputDefs; 3 special characters should do it)
      private readonly List<string> _outputDefs;  //same as ArbitraryOutputDefs, except that identified field token is replaced by a "magic token"
      private readonly List<string> _outputKeys;  //field names of items to include in output (null means void item)
      private readonly IItem _voidItem;

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      internal ArbitraryOutputProvider(OrchestratorConfig config, IGlobalCache globalCache) : base(config, globalCache)
      {
         //Parse ArbitraryOutputDefs to define _outputDefs and _outputKeys
         _outputDefs = new List<string>();  //output tokens with _magicToken instead of {..}
         _outputKeys = new List<string>();  //key to substitute the _magicToken (or null if no substitution)
         //Regex below matches entire row that contains a token surrounded by braces. Groups:
         //  ${1} - part before the first unescaped brace, i.e. {
         //  ${2} - part inside the first unescaped brace (aka token, i.e. the fields to be substituted)
         //  ${3} - part after the first closing brace, i.e. } that follows the first unescaped { (may contain additional braces { and/or })
         //Note that braces themselves are not captured in either group
         var regex = new Regex("^(.*?(?<!\\\\)){([^}]*)}(.*)$");  //note a negative lookbehind to NOT match \{ as an opening brace (\\\\ stands for a single \) (!)
         if (_config.ArbitraryOutputDefs != null)
         {
            foreach (var def in _config.ArbitraryOutputDefs)
            {
               if (def != null)  //null elements (if any) are ignored
               {
                  var match = regex.Match(def);
                  string outDef;  //def with magic token instead of a key to substitute
                  string outKey;  //key (of the value) to substitute the magic token
                  if (match.Success)
                  {
                     outDef = match.Groups[1].Value + _magicToken + match.Groups[3].Value;
                     outKey = match.Groups[2].Value;
                  }
                  else  //no match means no token to substitute (void item)
                  {
                     outDef = def;
                     outKey = null;
                  }
                  outDef = outDef.Replace("\\{", "{").Replace("\\}","}");  //replace escaped braces by literal braces
                  _outputDefs.Add(outDef);
                  _outputKeys.Add(outKey);
               }
            }
         }
         _voidItem = new VoidKeyValItem("irrelevant");  //key doesn't matter here
      }

      /// <summary>
      /// A function that substitutes a "magic token" by the actual item value
      /// </summary>
      internal override Func<IItem, int, string> TokenFromItem
      {
         get
         {
            return (itm, sNo) =>
            {
               return _outputDefs[sNo].Replace(_magicToken, itm.StringValue.TrimIfNeeded(_config.TrimOutputValues)?.QuoteIfNeeded(_config.QuotationMode.SurroundWithQuotes(itm.ItemDef.Type),_config.OutputFieldSeparator));
               //note that in case of a void item (non-existing key), "magic token" gets substituted by an empty string
            };
         }
      }


      /// <summary>
      /// A function that splices tokens (nothing in between)
      /// </summary>
      internal override Func<IEnumerable<string>, ExternalLine> TokenJoiner
      {
         get
         {
            return (tkns) => { return ExternalLine.CreateXtext(string.Concat(tkns)); };
         }
      }

      /// <summary>
      /// Extract tokens defined in ArbitraryOutputDefs from output record
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         foreach(var key in _outputKeys)
         {
            yield return key == null ? _voidItem : (IItem)outputRecord.GetItem(key);
         }
      }

      /// <summary>
      /// This function should never be called in case of arbitrary output
      /// </summary>
      internal override Func<string, int, string> TokenForHeaderRow
      {
         get
         {
            throw new InvalidOperationException("Header Row must not be applied to arbitrary output");
         }
      }


      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => throw new InvalidOperationException("ArbitraryOutputProvider must not use ExtItemFromItem function.");

   }
}
