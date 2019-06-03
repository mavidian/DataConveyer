//RawOutputProvider.cs
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
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// A set of functions specific to Raw data to be supplied to Output (Strategy Pattern)
   /// </summary>
   internal class RawOutputProvider : OutputProvider
   {
      //Raw data on output means that the item values are simply spliced together to form a line (keys are ignored)
      //In particular case where intake is also Raw and no items added in transformation, there will only be a single item named "RAW_REC"

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      internal RawOutputProvider(OrchestratorConfig config, IGlobalCache globalCache) : base(config, globalCache) { }

      /// <summary>
      /// A function that simply returns item value (field number doesn't matter here)
      /// </summary>
      internal override Func<IItem, int, string> TokenFromItem
      {
         get
         {
            return (itm, sNo) =>
            {
               return itm.StringValue;
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
      /// If no OutputFields specified, simply include all items in the record.
      /// If OutputFields are present, they drive items on output, possibly resulting in void items (if not present in the record).
      /// </summary>
      /// <param name="outputRecord"></param>
      /// <returns></returns>
      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         if (_config.OutputFields == null) return outputRecord.Items;
         //here, output is driven by OutputFields, i.e. FieldsToUse
         return base.ItemsFromFieldsToUse(outputRecord);
      }

      /// <summary>
      /// This function should never be called in case of raw output
      /// </summary>
      internal override Func<string, int, string> TokenForHeaderRow
      {
         get
         {
            throw new InvalidOperationException("Header Row should never be applied to raw output.");
         }
      }

      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => throw new InvalidOperationException("RawOutputProvider must not use ExtItemFromItem function.");

   }
}
