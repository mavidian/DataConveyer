//XrecordIntakeProvider.cs
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

namespace Mavidian.DataConveyer.Intake
{
   internal class XrecordIntakeProvider : IntakeProvider
   {
      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      internal XrecordIntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions) : base(config, globalCache, typeDefinitions) { }

      internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer => l => throw new InvalidOperationException("XrecordIntakeProvider must not use FieldTokenizer function.");

      internal override Func<string, int, IItem> ItemFromToken => (t,n) => throw new InvalidOperationException("XrecordIntakeProvider must not use ItemFromToken function.");

      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem
      {
         get
         {
            return t =>
            {  // t.Item1=key; t.Item2=value
               if (t.Item2 is string) return CreateItemAndMarkField(t.Item1, t.Item2 as string, _typeDefinitions);
               //here, val is expected to alredy be of the type intended (as per _typeDefinitions); no need to verify it, because
               //in case of a type mismatch, TypeDefinitions (CreateItemOf<T>) will convert it (worts case: default(T) will be used)
               return CreateItemAndMarkField(t.Item1, t.Item2, _typeDefinitions);
            };
         }
      }

   }
}
