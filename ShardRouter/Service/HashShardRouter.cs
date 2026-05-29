using ShardRouter.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShardRouter.Helpers;

public class HashShardRouter : IShardRouter
{
    private readonly string[] _connectionStrings;

    public HashShardRouter(string[] connectionStrings)
    {
        if (connectionStrings == null || connectionStrings.Length == 0)
            throw new ArgumentException("At least one connection string is required.", nameof(connectionStrings));

        _connectionStrings = connectionStrings;
    }

    public string GetConnectionString(int regionId)
    {
        var index = GetShardIndex(regionId);
        return _connectionStrings[index];
    }

    public int GetShardIndex(int regionId)
    {
        // Math.Abs handles negative regionId values safely
        return Math.Abs(regionId) % _connectionStrings.Length;
    }

    public IReadOnlyList<string> GetAllConnectionStrings()
        => Array.AsReadOnly(_connectionStrings);
}
