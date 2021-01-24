//ArbitraryIntakeProvider.cs
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
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Intake
{
   internal class ArbitraryIntakeProvider : IntakeProvider
   {

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      internal ArbitraryIntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions) : base(config, globalCache, typeDefinitions)
      {
         //Set FieldsInUse based on ArbitraryInputDefs (no on-the-fly fields are allowed)
         IncludeFieldsEnMasse(config.ArbitraryInputDefsBackingField.Select(t => t.Item1));
      }


      /// <summary>
      /// Function to extract tokens according to regex specified in ArbitraryInputDefs
      /// Each token is in the key=value format, where key is the field name and value is the corresponding value
      /// </summary>
      internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer
      {
         get
         {
            return line => { return line.Text.TokenizeUsingArbitraryDefs(_config.ArbitraryInputDefsBackingField); };
         }
      }


      internal override Func<string, int, IItem> ItemFromToken
      {
         get
         {
            return (t, i) => { return ItemFromKwToken(t, base._typeDefinitions); };
         }
      }


      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem => t => throw new InvalidOperationException("ArbitraryIntakeProvider must not use ItemFromExtItem function.");

   }
}
