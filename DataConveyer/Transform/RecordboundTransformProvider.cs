//RecordboundTransformProvider.cs
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


using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Transform
{
   internal class RecordboundTransformProvider : TransformProvider
   {

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="typeDefinitions"></param>
      /// <param name="config"></param>
      internal RecordboundTransformProvider(TypeDefinitions typeDefinitions, OrchestratorConfig config) : base(typeDefinitions, config) { }


      internal override Func<KeyValCluster, IEnumerable<KeyValCluster>> Transform
      {
         get
         {
            //A supplied function (which can be a default "pass-through" function if not supplied in config) is applied to every record in a cluster
            //and such transformed records form a new cluster, which in turn forms a one element sequence
            //However, if the supplied function returns null, such transformed record is excluded from the new cluster. In case all records are excluded
            // an empty enumerable is returned. Hence RecordboundTransformer can also act as RecordFilterTransformer.
            return clstr =>
            {
               var newRecs = clstr.Records.Select(rec => (KeyValRecord)_config.RecordboundTransformer(rec)).Where(rec => rec != null).ToList();
               //Note ToList above, which avoids double evaluation of RecordboundTransformer when newRecs is consumed below; this might have had
               // undesirable effect when the supplied transformer function returns the same (mutated) record instance.
               return newRecs.Any() ? Enumerable.Repeat(new KeyValCluster(newRecs, clstr.ClstrNo, clstr.StartRecNo, clstr.StartSourceNo, clstr.GlobalCache, clstr.PropertyBin, _typeDefinitions, _config, clstr._processingStatusSupplier), 1)
                                    : Enumerable.Empty<KeyValCluster>();
            };
            //TODO: Consider (??) AllowEmptyClusters config parameter, in which case (when true) empty cluster would be returned in case recList is empty (same idea as for RecordFilterTransformer).
         }
      }
   }
}
