//PerRecordRouter.cs
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


using Mavidian.DataConveyer.Entities.KeyVal;
using System;
using System.Linq;

namespace Mavidian.DataConveyer.Routing
{
   internal class PerClusterRouter : Router
   {
      private readonly Func<ICluster, int> _clusterRouter;

      internal PerClusterRouter(Func<ICluster,int> clusterRouter) : base()
      {
         _clusterRouter = clusterRouter;

      }  // ctor

      /// <summary>
      /// Use a user supplied function to determine the target number (same target for every record in the cluster)
      /// </summary>
      /// <param name="cluster"></param>
      /// <returns></returns>
      internal override ICluster Route(ICluster cluster)
      {
         int valToAssign = _clusterRouter(new ReadOnlyCluster((KeyValCluster)cluster));

         foreach (var rec in cluster.Records.Select(r => (KeyValRecord)r))
         {
            rec.TargetNo = valToAssign;
         }
         return cluster;
      }

   }
}
