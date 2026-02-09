#include "tsdb.hh"

#include <format>
#include <print>

struct Vec3 {
    f64 x, y, z;
};

template<>
struct std::formatter<Vec3> : std::formatter<std::string> {
    auto format(const Vec3& v, std::format_context& ctx) const {
        return std::format_to(ctx.out(), "Vec3({}, {}, {})", v.x, v.y, v.z);
    }
};

auto main() -> i32 {
    TSDB db {1};

    auto vec3_handle = db.register_struct(
    "Vec3", {
    	{"x", TSDB::F64},
    	{"y", TSDB::F64},
    	{"z", TSDB::F64},
    });

    db.insert(Vec3 { .x = 1, .y = 1, .z = 1}, vec3_handle);

    auto new_vec = db.query_first<Vec3>(vec3_handle);
    std::println("{}", new_vec);

    return 0;
}
