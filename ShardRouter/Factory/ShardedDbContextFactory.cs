using Microsoft.EntityFrameworkCore;
using ShardRouter.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShardRouter.Factory;

public sealed class ShardedDbContextFactory<TContext> where TContext : DbContext
{
    private readonly IShardRouter _shardRouter;
    private readonly Func<string, TContext> _contextFactory;

    /// <param name="shardRouter">The router that maps regionId to a connection string.</param>
    /// <param name="contextFactory">
    ///   A delegate that creates a TContext from a connection string.
    ///   Example: connStr => new OrderDbContext(new DbContextOptionsBuilder()
    ///                                              .UseSqlServer(connStr).Options)
    /// </param>
    public ShardedDbContextFactory(IShardRouter shardRouter, Func<string, TContext> contextFactory)
    {
        _shardRouter = shardRouter;
        _contextFactory = contextFactory;
    }

    /// <summary>
    /// Creates a DbContext pointing at the shard that owns the given regionId.
    /// The caller is responsible for disposing the context (use with "using").
    /// </summary>
    public TContext CreateForRegion(int regionId)
    {
        var connectionString = _shardRouter.GetConnectionString(regionId);
        return _contextFactory(connectionString);
    }

    /// <summary>
    /// Creates DbContext instances for ALL shards.
    /// Use for fan-out queries where you need data from every shard.
    /// </summary>
    public IEnumerable<TContext> CreateForAllShards()
    {
        return _shardRouter.GetAllConnectionStrings()
                           .Select(cs => _contextFactory(cs));
    }
}