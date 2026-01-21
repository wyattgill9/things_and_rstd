# Bringing Rust Option<T> & Result<T, E> into C++

```cpp
#include <string>
#include <vector>
#include <iostream>

#include "result.hh"
#include "utils.hh"

enum class Error {
    NotFound,
    InvalidInput,
    OutOfRange
};

struct User {
    int id;
    std::string name;
};

auto find_user(int id) -> Option<User> {
    if (id == 1) return Some(User{1, "Alice"});
    if (id == 2) return Some(User{2, "Bob"});
    return None;
}

auto parse_int(std::string_view s) -> Result<int, Error> {
    if (s.empty()) return Err(Error::InvalidInput);
    if (s == "42") return Ok(42);
    if (s == "100") return Ok(100);
    return Err(Error::NotFound);
}

auto divide(int a, int b) -> Result<int, Error> {
    if (b == 0) return Err(Error::InvalidInput);
    return Ok(a / b);
}

auto safe_get(const std::vector<int>& v, size_t i) -> Option<int> {
    if (i < v.size()) return Some(v[i]);
    return None;
}

int main() {
    auto opt_some = Some(42);
    auto opt_none = Option<int>(None);

    opt_some.is_some();
    opt_none.is_none();
    opt_some.is_some_and([](int x) { return x > 0; });
    opt_none.is_none_or([](int x) { return x > 0; });

    opt_some.unwrap_or(0);
    opt_none.unwrap_or(0);
    opt_none.unwrap_or_else([] { return -1; });

    auto doubled = opt_some.map([](int x) { return x * 2; });
    doubled.unwrap();

    auto str_len = opt_some
        .map([](int x) { return std::to_string(x); })
        .map([](const std::string& s) { return s.length(); });

    str_len.unwrap();

    auto chained = Some(5)
        .and_then([](int x) { return x > 0 ? Some(x * 10) : None; })
        .filter([](int x) { return x < 100; })
        .map([](int x) { return x + 1; });
    chained.unwrap();

    auto fallback = opt_none | Some(999);
    fallback.unwrap();

    auto lazy_fallback = opt_none.or_else([] { return Some(123); });
    lazy_fallback.unwrap();

    auto nested = Some(Some(42));
    std::move(nested).flatten().unwrap();

    auto zipped = Some(1).zip(Some(std::string("one")));
    auto [num, str] = std::move(zipped).unwrap();

    auto pair_opt = Some(std::pair{42, std::string("answer")});
    auto [opt_a, opt_b] = std::move(pair_opt).unzip();
    opt_a.unwrap();
    opt_b.unwrap();

    auto name = find_user(1)
        .map([](User u) { return u.name; });

    name.unwrap();

    auto result_from_opt = find_user(999)
        .ok_or(Error::NotFound);
    result_from_opt.is_err();

    auto res_ok = Result<int, Error>(Ok(42));
    auto res_err = Result<int, Error>(Err(Error::NotFound));

    res_ok.is_ok();
    res_err.is_err();
    res_ok.is_ok_and([](int x) { return x == 42; });
    res_err.is_err_and([](Error e) { return e == Error::NotFound; });

    res_ok.unwrap_or(0);
    res_err.unwrap_or(0);
    res_err.unwrap_or_else([](Error) { return -1; });

    parse_int("42")
        .map([](int x) { return x * 2; })
        .unwrap();

    parse_int("")
        .map_err([](Error) { return std::string("parse failed"); })
        .unwrap_err();

    parse_int("42")
        .and_then([](int x) { return divide(x, 2); })
        .map([](int x) { return x + 1; })
        .unwrap();

    parse_int("invalid")
        .or_else([](Error) { return Result<int, Error>(Ok(0)); })
        .unwrap();

    (parse_int("bad") | Result<int, Error>(Ok(999)))
        .unwrap();

    auto nested_result = Result<Result<int, Error>, Error>(
        Ok(Result<int, Error>(Ok(42)))
    );
    std::move(nested_result).flatten().unwrap();

    Result<int, Error>(Ok(42)).ok_value().unwrap();
    Result<int, Error>(Err(Error::NotFound)).err_value().unwrap();

    std::vector<int> vec{10, 20, 30};
    safe_get(vec, 0)
        .zip_with(safe_get(vec, 1), [](int a, int b) { return a + b; })
        .and_then([&](int s) {
            return safe_get(vec, 2)
                .map([s](int c) { return s + c; });
        })
        .unwrap();

    Option<int> mutable_opt = None;
    mutable_opt.get_or_insert(42);
    mutable_opt.unwrap();

    Option<int> to_take = Some(100);
    to_take.take().unwrap();
    to_take.is_none();

    Option<int> to_replace = Some(1);
    to_replace.replace(2).unwrap();
    to_replace.unwrap();

    Some(50)
        .take_if([](int& x) { return x > 25; })
        .unwrap();

    (Some(1) ^ Option<int>(None)).unwrap();
    (Some(1) ^ Some(2)).is_none();

    bool inspected = false;
    Some(42).inspect([&](int) { inspected = true; });

    bool err_inspected = false;
    Result<int, Error>(Err(Error::NotFound))
        .inspect_err([&](Error) { err_inspected = true; });

    auto complex_chain = find_user(2)
        .map([](User u) { return u.id; })
        .ok_or(Error::NotFound)
        .and_then([](int id) { return divide(100, id); })
        .map([](int x) { return std::to_string(x); })
        .unwrap_or("error");

    std::cout << complex_chain << "\n";

    return 0;
}
```
