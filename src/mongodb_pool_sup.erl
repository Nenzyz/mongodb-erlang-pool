-module(mongodb_pool_sup).

-behaviour(supervisor).

%% Include
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, start_link/2]).
-export([create_pool/3, create_pool/4, delete_pool/1]).
-export([add_pool/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================
%% start_link() ->
%%   case application:get_env(mongodb_pool, pools) of
%%     {ok, Pools} ->
%%       {ok, GlobalOrLocal} = application:get_env(mongodb_pool, global_or_local),
%%       Dets = get(mongo_pools_dets),
%%       Pools = [{PoolName, Size, Params} || {_,{PoolName, Size, Params}} <- dets:foldl(fun(X, L) -> [X|L] end, [], Dets)],
%%       start_link(Pools, GlobalOrLocal);
%%     _ ->
%%       supervisor:start_link({local, ?MODULE}, ?MODULE, [[], local])
%%   end.

start_link() ->
  {ok, GlobalOrLocal} = application:get_env(mongodb_pool, global_or_local),
  [{mongo_pools_dets, Schemas}]= ets:lookup(tirate_stats, mongo_pools_dets),
  error_logger:info_msg("Found tirate_stats link to schemas.dets: ~p", [Schemas]),
  Pools = [{PoolName, Size, Params} || {_,{PoolName, Size, Params}} <- dets:foldl(fun(X, L) -> [X|L] end, [], Schemas)],
  ets:insert(mongo_schemas, [{list_to_atom(SName), true} || {SName,_} <- dets:foldl(fun(X, L) -> [X|L] end, [], Schemas)]),
  error_logger:info_msg("Found mongo pools: ~p", [Pools]),
  start_link(Pools, GlobalOrLocal).

start_link(Pools, GlobalOrLocal) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [Pools, GlobalOrLocal]).

%% ===================================================================
%% @doc create new pool.
%% @end
%% ===================================================================
-spec(create_pool(PoolName::atom(), Size::integer(), Options::[tuple()]) ->
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size, Options) ->
    create_pool(local, PoolName, Size, Options).

%% ===================================================================
%% @doc create new pool, selectable name zone global or local.
%% @end
%% ===================================================================
-spec(create_pool(GlobalOrLocal::atom(), PoolName::atom(), Size::integer(), Options::[tuple()]) ->
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(GlobalOrLocal, PoolName, Size, Options) 
  when GlobalOrLocal =:= local;
       GlobalOrLocal =:= global ->

    SizeArgs = [{size, Size}, {max_overflow, 10}],
    PoolArgs = [{name, {GlobalOrLocal, PoolName}}, {worker_module, mc_worker}],
    PoolSpec = poolboy:child_spec(PoolName, PoolArgs ++ SizeArgs, Options),

    supervisor:start_child(?MODULE, PoolSpec).

%% ===================================================================
%% @doc delete pool
%% @end
%% ===================================================================
-spec(delete_pool(PoolName::atom()) -> ok | {error,not_found}).

delete_pool(PoolName) ->
    supervisor:terminate_child(?MODULE, PoolName),
    supervisor:delete_child(?MODULE, PoolName).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Pools, GlobalOrLocal]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {GlobalOrLocal, Name}},
                    {worker_module, mc_worker}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),

    error_logger:info_msg("Started mongodb pool ~p ~p", [SupFlags, PoolSpecs]),
    {ok, {SupFlags, PoolSpecs}}.

add_pool(SchemaName) ->
  %% TODO: rewrite default pool definition to get from DETS table and overwrite with new values
  error_logger:info_msg("Adding new MongoDB pool: ~p", [SchemaName]),
  {ok, DefaultWorkers} = application:get_env(ti_boss, default_mongo_pool_size),
  {ok, DefaultMongoProfile} =  application:get_env(ti_boss, default_mongo_pool),
  DMP = dict:to_list(dict:store(database, ti_task:ensure_binary(SchemaName), dict:from_list(DefaultMongoProfile))),
%%   ChildSpec = #{id => list_to_atom("mpool_" ++ SchemaName),
%%     modules => [poolboy],
%%     restart => permanent,
%%     shutdown => 5000,
%%     start => {poolboy,start_link,
%%       [[{name,{local,list_to_atom("mpool_" ++ SchemaName)}},
%%         {worker_module,mc_worker}] ++ DefaultWorkers,
%%         dict:to_list(DMP)]},
%%     type => worker},
  PChildSpec = poolboy:child_spec(list_to_atom("mpool_" ++ SchemaName), [{name,{local,list_to_atom("mpool_" ++ SchemaName)}},
    {worker_module,mc_worker}] ++ DefaultWorkers, DMP),
  [{mongo_pools_dets, Schemas}]= ets:lookup(tirate_stats, mongo_pools_dets),
  ok = dets:insert(Schemas, {SchemaName, {list_to_atom("mpool_" ++ SchemaName), [{name,{local,list_to_atom("mpool_" ++ SchemaName)}},
    {worker_module,mc_worker}] ++ DefaultWorkers, DMP}}),
  ets:insert(mongo_schemas, [{list_to_atom(SchemaName), true}]),
  error_logger:info_msg("Starting mongodb pool ~p", [PChildSpec]),
  supervisor:start_child(?MODULE, PChildSpec).