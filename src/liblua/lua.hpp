#ifndef LIB_LUA_LUA_HPP
#define LIB_LUA_LUA_HPP

#include "include/types.h"

extern "C" {
#include "liblua/src/lua.h"
#include "liblua/src/lualib.h"
#include "liblua/src/lauxlib.h"
}

extern "C" {
  LUALIB_API int luaopen_cmsgpack(lua_State *L);
}

LUALIB_API int luaopen_bufferlist(lua_State *L);

extern bufferlist *clslua_checkbufferlist(lua_State *L, int pos = 1);
extern bufferlist *clslua_pushbufferlist(lua_State *L, bufferlist *set);

#endif
