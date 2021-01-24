//KwOutputProvider.cs
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
   /// <summary>
   /// A set of functions specific to KW data to be supplied to the Output part of the EtlOrchestrator (Strategy Pattern)
   /// </summary>
   internal class KwOutputProvider : OutputProvider
   {
      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      internal KwOutputProvider(OrchestratorConfig config, IGlobalCache globalCache) : base(config, globalCache) { }

      /// <summary>
      /// A function to concatenate parts of a KeyValItem to a form Key="Value"
      /// </summary>
      internal override Func<IItem, int, string> TokenFromItem
      {
         get
         {
            return (itm, sNo) =>
            {
               string keyPart = _config.OutputKeyPrefix == null ? itm.Key : _config.OutputKeyPrefix + itm.Key;
               if (itm.Value == null) return keyPart;
               return keyPart + "=" + itm.StringValue.TrimIfNeeded(_config.TrimOutputValues).QuoteIfNeeded(_config.QuotationMode.SurroundWithQuotes(itm.ItemDef.Type),_config.OutputFieldSeparator);
            };
         }
      }

      /// <summary>
      /// A function to splice tokens with a separator character in between
      /// </summary>
      internal override Func<IEnumerable<string>, ExternalLine> TokenJoiner
      {
         get
         {
            return (tkns) => { return ExternalLine.CreateXtext(string.Join(_config.OutputFieldSeparator.ToString(), tkns)); };
         }
      }

      /// <summary>
      /// If no OutputFields specified, simply include all items in the record.
      /// If OutputFields are present, they drive items on output, possibly resulting in void items (if not present in the record); however, if ExcludeExtraneousFields then such void items are excluded.
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         if (_config.OutputFields == null) return outputRecord.Items;
         //here, output is driven by OutputFields, i.e. FieldsToUse
         var allRequestedItems =  base.ItemsFromFieldsToUse(outputRecord);
         if (_config.ExcludeExtraneousFields) return allRequestedItems.Where(itm => itm.ItemDef.Type != ItemType.Void);
         return allRequestedItems;       
      }

      /// <summary>
      /// This function should never be called in case of KW output
      /// </summary>
      internal override Func<string,int,string> TokenForHeaderRow
      {
         get
         {
            throw new InvalidOperationException("Header Row should never be applied to KW output");
         }
      }

      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => throw new InvalidOperationException("KwOutputProvider must not use ExtItemFromItem function.");

   }
}
