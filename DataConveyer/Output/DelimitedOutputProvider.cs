//DelimitedOutputProvider.cs
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
using Mavidian.DataConveyer.Intake;        //to "borrow" TrimIfNeeded extension method that resides in IntakeHelpers
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Output
{
   internal class DelimitedOutputProvider : OutputProvider
   {
      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      internal DelimitedOutputProvider(OrchestratorConfig config, IGlobalCache globalCache) : base(config, globalCache) { }

      /// <summary>
      /// A function to return a string representation of the item's Value (possibly surrounded by quotes)
      /// </summary>
      internal override Func<IItem, int, string> TokenFromItem
      {
         get
         {
            return (itm, sNo) => CreateDelimitedToken(itm.StringValue, itm.ItemDef.Type);
         }
      }

      /// <summary>
      /// Delimited TokenJoiner: a function to splice tokens with a separator in between
      /// </summary>
      internal override Func<IEnumerable<string>, ExternalLine> TokenJoiner
      {
         get
         {
            return (tkns) => { return ExternalLine.CreateXtext(string.Join(_config.OutputFieldSeparator.ToString(), tkns)); };
         }
      }

      /// <summary>
      /// Provide a list of items to include in output based on FieldsToUse (insignificant, trailing items may be removed based on ExcludeExtranousFields setting)
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         if (_config.ExcludeExtraneousFields)
         {
            //remove trailing, empty fields
            return base.ItemsFromFieldsToUse(outputRecord).Reverse().SkipWhile(itm => string.IsNullOrEmpty(itm.StringValue)).Reverse();
         }
         return base.ItemsFromFieldsToUse(outputRecord);
      }

      /// <summary>
      /// Returns a token to become a part of the header row
      /// </summary>
      internal override Func<string, int, string> TokenForHeaderRow
      {
         get
         {
            return (fn, sNo) => CreateDelimitedToken(fn, ItemType.String);
         }
      }


      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => throw new InvalidOperationException("DelimitedOutputProvider must not use ExtItemFromItem function.");


      /// <summary>
      /// Helper function to take a "raw" token value and apply needed trimming and/or quoting
      /// </summary>
      /// <param name="value"></param>
      /// <param name="type"></param>
      /// <returns></returns>
      private string CreateDelimitedToken(string value, ItemType type)
      {
         return value?.TrimIfNeeded(_config.TrimOutputValues)?.QuoteIfNeeded(_config.QuotationMode.SurroundWithQuotes(type),_config.OutputFieldSeparator);
      }

   }
}
