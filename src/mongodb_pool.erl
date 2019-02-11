-module(mongodb_pool).

%% Include
-include_lib("eunit/include/eunit.hrl").
-include("../../deps/mongodb/include/mongo_protocol.hrl").

%% API
-export([start/0, stop/0]).
-export([create_pool/4, delete_pool/1]).
-export([
         find/2,
         find/3,
         find/4,
         find_one/2,
         find_one/3,
         find_one/4,
         % find_one/5,
         insert/2,
         insert/3,
         update/2,
         update/4,
         update/5,
         % update/6,
         delete/2,
         delete/3,
         delete_one/2,
         delete_one/3
        ]).
-export([
         count/2,
         count/3,
         count/4
        ]).
-export([
         command/2,
         command/3,
         ensure_index/3,
         ensure_index/4
        ]).
-export([
         do/2
        ]).



%%%===================================================================
%%% API functions (shortcuts)
%%%===================================================================

start() ->
  application:start(?MODULE).

stop() ->
  application:stop(?MODULE).

-spec(create_pool(GlobalOrLocal::atom(), PoolName::atom(), Size::integer(), Options::[tuple()]) ->
         {ok, pid()} | {error,{already_started, pid()}}).

create_pool(GlobalOrLocal, Size, PoolName, Options) ->
  mongodb_pool_sup:create_pool(GlobalOrLocal, Size, PoolName, Options).

-spec(delete_pool(PoolName::atom()) -> ok | {error,not_found}).
delete_pool(PoolName) ->
  mongodb_pool_sup:delete_pool(PoolName).


%% MongoDB Interface shortcuts

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(term(), mc_worker_api:collection(), A) -> A.
insert(_PoolName, _Coll, []) ->  
  [];
insert(PoolName, Coll, Docs) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:insert(Connection, Coll, Docs)
                        end).

-spec insert(term(), map()) -> term().
insert(PoolName, Map = #{connection := _, collection := _, doc := _}) ->  
  do(PoolName, fun(_) ->
                            mc_worker_api:insert(Map)
                        end).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(term(), map()) -> ok.
update(PoolName, Map = #{connection := _, collection := _, selector := _, doc := _}) ->
  do(PoolName, fun(_) ->
                            mc_worker_api:update(Map)
                        end).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(term(), mc_worker_api:collection(), mc_worker_api:selector(), bson:document()) -> ok.
update(PoolName, Coll, Selector, Doc) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:update(Connection, Coll, Selector, Doc)
                        end).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(term(), mc_worker_api:collection(), mc_worker_api:selector(), bson:document(), boolean()) -> ok.
update(PoolName, Coll, Selector, Doc, Upsert) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:update(Connection, Coll, Selector, Doc, Upsert)
                        end).

%% @doc Replace the document matching criteria entirely with the new Document.
% -spec update(term(), mc_worker_api:collection(), mc_worker_api:selector(), bson:document(), boolean(), boolean()) -> ok.
% update(PoolName, Coll, Selector, Doc, Upsert, MultiUpdate) ->
%   do(PoolName, fun(Connection) ->
%                             mc_worker_api:update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate)
%                         end).

%% @doc Delete selected documents
-spec delete(term(), mc_worker_api:collection(), mc_worker_api:selector()) -> ok.
delete(PoolName, Coll, Selector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:delete(Connection, Coll, Selector)
                        end).

%% @doc Delete selected documents
-spec delete(term(), map()) -> ok.
delete(PoolName, Map = #{connection := _, collection := _, selector := _}) ->
  do(PoolName, fun(_) ->
                            mc_worker_api:delete(Map)
                        end).

%% @doc Delete first selected document.
-spec delete_one(term(), mc_worker_api:collection(), mc_worker_api:selector()) -> ok.
delete_one(PoolName, Coll, Selector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:delete_one(Connection, Coll, Selector)
                        end).

%% @doc Delete first selected document.
-spec delete_one(term(), map()) -> ok.
delete_one(PoolName, Map = #{connection := _, collection := _, selector := _}) ->
  delete(PoolName, Map#{limit => 1}).

%% @doc Delete first selected document.
-spec find(term(), map()) -> ok.
find(PoolName, Map = #{connection := _, collection := _, selector := _}) ->
  do(PoolName, fun(_) ->
                            mc_worker_api:find(Map)
                        end).

%% @doc Delete first selected document.
-spec find(term(), mc_worker_api:collection(), mc_worker_api:selector()) -> ok.
find(PoolName, Coll, Selector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:find(Connection, Coll, Selector)
                        end).
%% -spec find(term(), mc_worker_api:collection(), mc_worker_api:selector(), mc_worker_api::projector()) -> ok.
find(PoolName, Coll, Selector, Projector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:find(Connection, Coll, Selector, Projector)
                        end).


%% @doc Return first selected document, if any
-spec find_one(term(), map()) -> {} | {bson:document()}.
find_one(PoolName, Map = #{connection := _, collection := _, selector := _}) ->
  do(PoolName, fun(_) ->
                            mc_worker_api:find_one(Map)
                        end).

%% @doc Return first selected document, if any
-spec find_one(term(), mc_worker_api:collection(), mc_worker_api:selector()) -> {} | {bson:document()}.
find_one(PoolName, Coll, Selector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:find_one(Connection, Coll, Selector)
                        end).

%% @doc Return projection of first selected document, if any. Empty projection [] means full projection.
-spec find_one(term(), mc_worker_api:collection(), mc_worker_api:selector(), mc_worker_api:projector()) -> {} | {bson:document()}.
find_one(PoolName, Coll, Selector, Projector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:find_one(Connection, Coll, Selector, Projector)
                        end).

%% @doc Return projection of Nth selected document, if any. Empty projection [] means full projection.
% -spec find_one(term(), mc_worker_api:collection(), mc_worker_api:selector(), mc_worker_api:projector(), mc_worker_api:skip()) -> {} | {bson:document()}.
% find_one(PoolName, Coll, Selector, Projector, Skip) ->
%   do(PoolName, fun(Connection) ->
%                             mc_worker_api:find_one(Connection, Coll, Selector, Projector, Skip)
%                         end).

%@doc Count selected documents
-spec count(term(), map()) -> integer().
count(PoolName, Map = #{connection := _, collection := _, selector := _}) ->
  do(PoolName, fun(_) ->
                          mc_worker_api:count(Map)
                      end).

%@doc Count selected documents
-spec count(term(), mc_worker_api:collection(), mc_worker_api:selector()) -> integer().
count(PoolName, Coll, Selector) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:count(#{connection => Connection, collection => Coll, selector => Selector})
                        end).

%@doc Count selected documents up to given max number; 0 means no max.
%     Ie. stops counting when max is reached to save processing time.
-spec count(term(), mc_worker_api:collection(), mc_worker_api:selector(), integer()) -> integer().
count(PoolName, Coll, Selector, Limit) -> 
  do(PoolName, fun(Connection) ->
                            mc_worker_api:count(#{connection => Connection, collection => Coll, selector => Selector, limit => Limit})
                        end).

%% TBD: add Database to count. command...

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()
-spec ensure_index(term(), mc_worker_api:collection(), bson:document()) -> ok.
ensure_index(PoolName, Coll, IndexSpec) ->  
  do(PoolName, fun(Connection) ->
                            mc_worker_api:ensure_index(Connection, Coll, IndexSpec)
                        end).

%% @doc Create index on collection according to given spec.
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      name     :: bson:utf8()
%%      unique   :: boolean()
%%      dropDups :: boolean()
-spec ensure_index(term(), mc_worker_api:collection(), bson:document(), term()) -> ok.
ensure_index(PoolName, Coll, IndexSpec, DB) ->  
  do(PoolName, fun(Connection) ->
                            mc_worker_api:ensure_index(Connection, Coll, IndexSpec, DB)
                        end).

%% @doc Execute given MongoDB command and return its result.
-spec command(term(), bson:document()) -> {boolean(), bson:document()}. % Action
command(PoolName, Command) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:command(Connection, Command)
                        end).

%% @doc Execute given MongoDB command and return its result specifying DB.
-spec command(term(), term(), bson:document()) -> {boolean(), bson:document()}. % Action
command(PoolName, Database, Command) ->
  do(PoolName, fun(Connection) ->
                            mc_worker_api:command(Database, Connection, Command)
                        end).

-spec do(term(), mc_worker_api:action(A)) -> A.

do(PoolName, Fun) ->
  poolboy:transaction(PoolName, Fun).
