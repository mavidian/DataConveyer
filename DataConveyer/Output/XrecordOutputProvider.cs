//XrecordOutputProvider.cs
//
// Copyright © 2018-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   internal class XrecordOutputProvider : OutputProvider
   {
      private readonly ItemType _typesToConvertToString;

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="typesToConvertToString">Flags set for item types to be converted to string.</param>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      internal XrecordOutputProvider(ItemType typesToConvertToString, OrchestratorConfig config, IGlobalCache globalCache) : base(config, globalCache)
      {
         _typesToConvertToString = typesToConvertToString;
         //For XML, all types are converted to string - this way the format (if set in ItemDef) is applied on output.
         //For JSON, all types except for DateTime are NOT converted to string; this is because JSON natively represents data type (however, it doesn't support dates).
         //Note that XrecordOutputProvider could be split into 2 separate output providers (XmlOutputProvider and JsonOutputProvider), in which case
         //no typesToConvertToString would be needed (and it would directly fit into the strategy pattern).
         //However, a single XrecordOutputProvider class allows for "symmetry" with Intake, where there is a single XrecordIntakeProvider.
         //TODO: Consider splitting XrecordOutputProvider into XmlOutputProvider and JsonOutputProvider if performance slowdown is suspected.
      }

      internal override Func<IEnumerable<string>, ExternalLine> TokenJoiner => ts => throw new InvalidOperationException("XrecordOutputProvider must not use TokenJoiner function.");

      internal override Func<IItem, int, string> TokenFromItem => (i,n) => throw new InvalidOperationException("XrecordOutputProvider must not use TokenFromItem function.");


      internal override IEnumerable<IItem> ItemsToOutput(KeyValRecord outputRecord)
      {
         if (_config.OutputFields == null) return outputRecord.Items;
         //here, output is driven by OutputFields, i.e. FieldsToUse
         return base.ItemsFromFieldsToUse(outputRecord);
      }


      internal override Func<string, int, string> TokenForHeaderRow => (f,n) => throw new InvalidOperationException("XrecordOutputProvider must not use TokenForHeaderRow function.");


      //Either use the StringValue property (which converts to string respecting format) or the Value property (which returns the "raw", typed value)
      internal override Func<IItem, Tuple<string, object>> ExtItemFromItem => i => Tuple.Create(i.Key, ConvertToString(i) ? i.StringValue as object : i.Value);


      /// <summary>
      /// Determine if a given item needs to be converted to string or not
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      private bool ConvertToString(IItem item)
      {
         var type = item.ItemDef.Type;
         return (type & _typesToConvertToString) == type;
      }

   }

}
