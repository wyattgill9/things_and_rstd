#pragma once

#include "absl/container/flat_hash_map.h"

#include "utils.hh"

#include <cstring>
#include <utility>
#include <vector>
#include <string>
#include <initializer_list>

template <std::unsigned_integral T>
[[nodiscard]] constexpr auto align_up(T value, T alignment) noexcept -> T {
    assert(alignment != 0);
    assert(std::has_single_bit(alignment));

    return (value + alignment - 1) & ~(alignment - 1);
}

class Schema;
class TSDB;

struct TypeHandle {
    constexpr TypeHandle(u32 v) : v_(v) {}

    constexpr friend bool operator==(TypeHandle, TypeHandle) = default;

    template <typename H>
    friend H AbslHashValue(H h, const TypeHandle& t) {
        return H::combine(std::move(h), t.v_);
    }

private:
    friend class Schema;
    friend class TSDB;

    u32 v_;
};

class Schema {
public:
    enum class TypeKind : u8 {
        U8, U16, U32, U64,
        I8, I16, I32, I64,
        F32, F64,
        BOOL,
        TIMESTAMP_NS,
        STRUCT,
    };

    struct Field {
        std::string name;
        TypeHandle  type;
        u32         offset = 0;
    };

    struct TypeMeta {
        std::string        name;
        TypeKind           kind;
        u32                size      = 0;
        u32                alignment = 1;
        std::vector<Field> fields;
    };

    Schema(size_t est_num_types) { init_schema(est_num_types); }

    auto register_struct(std::string name,
                         std::initializer_list<std::pair<std::string, const TypeHandle>> fields) -> TypeHandle
    {
        TypeMeta type {
            .name = std::move(name),
            .kind = TypeKind::STRUCT,
        };

        type.alignment = 8;
        type.size      = 8;
        type.fields.push_back({ "timestamp_ns", { static_cast<u32>(TypeKind::TIMESTAMP_NS) }, 0 });

        for (auto&& [field_name, handle] : fields) {
            const auto& ft = meta_of(handle);
            type.alignment  = std::max(type.alignment, ft.alignment);
            type.size       = align_up(type.size, ft.alignment);
            type.fields.push_back({ field_name, handle, type.size });
            type.size += ft.size;
        }

        type.size = align_up(type.size, type.alignment);

        const TypeHandle result { static_cast<u32>(types_.size()) };
        types_.push_back(std::move(type));
        return result;
    }

    [[nodiscard]] auto meta_of(TypeHandle h) const -> const TypeMeta&  { return types_[h.v_]; }

private:
    void init_schema(size_t est_num_types) {
        constexpr std::pair<std::string_view, TypeKind> prims[] = {
            {"u8",  TypeKind::U8},  {"u16", TypeKind::U16}, {"u32", TypeKind::U32}, {"u64", TypeKind::U64},
            {"i8",  TypeKind::I8},  {"i16", TypeKind::I16}, {"i32", TypeKind::I32}, {"i64", TypeKind::I64},
            {"f32", TypeKind::F32}, {"f64", TypeKind::F64},
            {"bool", TypeKind::BOOL},
            {"timestamp_ns", TypeKind::TIMESTAMP_NS},
        };

        constexpr u32 sizes[] = {
            1, 2, 4, 8,
            1, 2, 4, 8,
            4, 8,
            1,
            8,
        };

        for (u32 i = 0; i < std::size(prims); ++i) {
            types_.push_back(TypeMeta {
                .name      = std::string(prims[i].first),
                .kind      = prims[i].second,
                .size      = sizes[i],
                .alignment = sizes[i],
            });
        }

        types_.reserve(types_.size() + est_num_types);
    }

    std::vector<TypeMeta> types_;
};

struct Column {
public:
    Column() = default;
    explicit Column(size_t elem_size) : elem_size_(elem_size) {}

    auto push(const std::byte* data) -> void {
        const size_t old_size = data_.size();
        data_.resize(old_size + elem_size_);
        std::memcpy(data_.data() + old_size, data, elem_size_);
    }

    [[nodiscard]] auto at(size_t row) const -> const std::byte* {
        return data_.data() + row * elem_size_;
    }

    [[nodiscard]] auto row_count() const -> size_t {
        return elem_size_ > 0 ? data_.size() / elem_size_ : 0;
    }

    [[nodiscard]] auto elem_size() const -> size_t { return elem_size_; }

    auto reserve(size_t row_count) -> void {
        data_.reserve(row_count * elem_size_);
    }

private:
    size_t elem_size_ = 0;
    std::vector<std::byte> data_;
};

struct Table {
public:
    Table(std::vector<size_t> field_sizes, std::vector<size_t> field_offsets)
        : field_offsets_(std::move(field_offsets))
    {
        columns_.reserve(field_sizes.size());
        for (size_t sz : field_sizes) {
            columns_.emplace_back(sz);
        }
    }

    auto insert_row(const std::byte* src) -> void {
        for (size_t i = 0; i < columns_.size(); ++i) {
            columns_[i].push(src + field_offsets_[i]);
        }
        ++row_count_;
    }

    auto read_row(size_t row, std::byte* dst) const -> void {
        for (size_t i = 0; i < columns_.size(); ++i) {
            std::memcpy(dst + field_offsets_[i], columns_[i].at(row), columns_[i].elem_size());
        }
    }

    auto reserve(size_t row_count) -> void {
        for (auto& col : columns_) {
            col.reserve(row_count);
        }
    }

    [[nodiscard]] auto row_count() const -> size_t { return row_count_; }

private:
    size_t row_count_ = 0;
    std::vector<size_t> field_offsets_;
    std::vector<Column> columns_;
};

class TSDB {
public:
    TSDB(size_t est_num_types = 1) : schema_(est_num_types) {}

    ~TSDB()           = default;
    TSDB(const TSDB&) = delete;
    TSDB(TSDB&&)      = delete;

    auto register_struct(std::string name,
                         std::initializer_list<std::pair<std::string, const TypeHandle>> fields) -> TypeHandle
    {
        return schema_.register_struct(name, fields);
    }

    template<typename T>
    auto insert(const T& src, TypeHandle type) -> void {
        static_assert(std::is_trivially_copyable_v<T>);

        Table& table = get_or_create_table(type);
        const auto* bytes = reinterpret_cast<const std::byte*>(&src);

        table.insert_row(bytes);
    }

    template<typename T>
    [[nodiscard]] auto query_first(TypeHandle type) const -> T {
        static_assert(std::is_trivially_copyable_v<T>);

        const Table* table = get_table_ptr(type);
        if (table == nullptr || table->row_count() == 0) {
            return T{};
        }

        T result {};
        auto* dst = reinterpret_cast<std::byte*>(&result);
        table->read_row(0, dst);

        return result;
    }

    // Default Types
    constexpr static TypeHandle U8   { std::to_underlying(Schema::TypeKind::U8  ) };
    constexpr static TypeHandle U16  { std::to_underlying(Schema::TypeKind::U16 ) };
    constexpr static TypeHandle U32  { std::to_underlying(Schema::TypeKind::U32 ) };
    constexpr static TypeHandle U64  { std::to_underlying(Schema::TypeKind::U64 ) };
    constexpr static TypeHandle I8   { std::to_underlying(Schema::TypeKind::I8  ) };
    constexpr static TypeHandle I16  { std::to_underlying(Schema::TypeKind::I16 ) };
    constexpr static TypeHandle I32  { std::to_underlying(Schema::TypeKind::I32 ) };
    constexpr static TypeHandle I64  { std::to_underlying(Schema::TypeKind::I64 ) };
    constexpr static TypeHandle F32  { std::to_underlying(Schema::TypeKind::F32 ) };
    constexpr static TypeHandle F64  { std::to_underlying(Schema::TypeKind::F64 ) };
    constexpr static TypeHandle BOOL { std::to_underlying(Schema::TypeKind::BOOL) };

    constexpr static TypeHandle TIME_NS { std::to_underlying(Schema::TypeKind::TIMESTAMP_NS) };

private:
    [[nodiscard]] auto get_table_ptr(TypeHandle type) const -> const Table* {
        auto it = tables_.find(type);
        if (it != tables_.end()) return &it->second;
        return nullptr;
    }

    [[nodiscard]] auto get_or_create_table(TypeHandle type) -> Table& {
        if (auto it = tables_.find(type); it != tables_.end()) {
            return it->second;
        }

        auto&& fields = schema_.meta_of(type).fields;

        auto offsets = fields
            | std::views::transform([](auto& f) { return f.offset; })
            | std::ranges::to<std::vector<u32>>();

        auto sizes = fields
            | std::views::transform([&](auto& f) { return schema_.meta_of(f.type).size; })
            | std::ranges::to<std::vector<size_t>>();

        return tables_.emplace(type, Table {std::move(sizes), {offsets.begin(), offsets.end()}}).first->second;
    }

    Schema schema_;
    absl::flat_hash_map<TypeHandle, Table> tables_;
};
