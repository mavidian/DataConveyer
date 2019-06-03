//RawIntakeProvider.cs
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
using System.Linq;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// A set of functions specific to Raw data to be supplied to Intake (Strategy Pattern)
   /// </summary>
   internal class RawIntakeProvider : IntakeProvider
   {
      //Raw data on intake means that the entire line contents becomes a value of a single item

      private const string RawKey = "RAW_REC";  // name of the sole field in raw data - "magic" value

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      internal RawIntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions) : base(config, globalCache, typeDefinitions)
      {
         //Set the one (and only one) field in FieldsInUse (no on-the-fly fields are allowed)
         IncludeField(RawKey);
      }


   internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer
      {
         get
         {
            return line => Enumerable.Repeat(line.Text, 1);
         }
      }

      internal override Func<string, int, IItem> ItemFromToken
      {
         get
         {
            return (t, i) =>
            {
               return CreateItemAndMarkField(RawKey, t, base._typeDefinitions);
            };
         }
      }


      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem => t => throw new InvalidOperationException("RawIntakeProvider must not use ItemFromExtItem function.");

   }
}
