using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShardRouter.Interface;

public interface IShardRouter
{
    string GetConnectionString(int regionId);
    int GetShardIndex(int regionId);
    IReadOnlyList<string> GetAllConnectionStrings();
}
