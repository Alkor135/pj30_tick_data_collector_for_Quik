-- Выгружает обезличенные сделки ближайших фьючерсов RTS и MIX из QUIK.
-- Читает новые строки all_trades и публикует пакеты в папке spool рядом со скриптом.
-- Выбирает ближайшую серию по mat_date и переключается в 19:00 МСК в день экспирации.
-- Пример запуска: запустите .\.venv\Scripts\python.exe .\collect_ticks.py,
-- затем добавьте export_ticks.lua в окне Lua-скриптов QUIK и нажмите «Запустить».

local CLASS_CODE = "SPBFUT"
local CONTRACT_PATTERN = "^[A-Z][A-Z][FGHJKMNQUVXZ]%d$"
local EXPIRY_TIME = "T19:00:00.000000"
local SCAN_SIZE = 2000
local running = true
local sequence = 0

-- Определяет корень по коду стандартного фьючерса; возвращает RI, MX или nil.
local function contract_root(code)
    if not code or not code:match(CONTRACT_PATTERN) then
        return nil
    end
    local root = code:sub(1, 2)
    if root == "RI" or root == "MX" then
        return root
    end
end

-- Возвращает отсортированные серии и признак готовности справочника QUIK.
-- known хранит известные даты, в том числе для серий, исчезнувших из текущего списка.
local function refresh_contracts(known)
    local ready = {RI = true, MX = true}
    for code in (getClassSecurities(CLASS_CODE) or ""):gmatch("[^,]+") do
        local root = contract_root(code)
        if root then
            local info = getSecurityInfo(CLASS_CODE, code)
            local expiry = info and tostring(info.mat_date):gsub("%.0+$", ""):match("^%d%d%d%d%d%d%d%d$")
            if expiry then
                local day = expiry:sub(1, 4) .. "-" .. expiry:sub(5, 6) .. "-" .. expiry:sub(7, 8)
                known[root][code] = day .. EXPIRY_TIME
            elseif not known[root][code] then
                ready[root] = false
            end
        end
    end

    local contracts = {RI = {}, MX = {}}
    for root, group in pairs(known) do
        for code, expiry in pairs(group) do
            contracts[root][#contracts[root] + 1] = {code = code, expiry = expiry}
        end
        -- Сравнивает даты экспирации двух серий, при равенстве сравнивает их коды.
        table.sort(contracts[root], function(first, second)
            if first.expiry == second.expiry then
                return first.code < second.code
            end
            return first.expiry < second.expiry
        end)
        ready[root] = ready[root] and #contracts[root] > 0
    end
    return contracts, ready
end

-- Возвращает ближайший действующий код из списка серий на биржевое время timestamp.
local function front_contract(contracts, timestamp)
    for _, contract in ipairs(contracts) do
        if timestamp < contract.expiry then
            return contract.code
        end
    end
end

-- Преобразует время сделки из QLua в ISO-строку с шестью знаками дробной части.
local function trade_timestamp(stamp)
    local micros = stamp.mcs
    if not micros or micros == 0 then
        micros = (stamp.ms or 0) * 1000
    end
    return string.format("%04d-%02d-%02dT%02d:%02d:%02d.%06d",
        stamp.year, stamp.month, stamp.day, stamp.hour, stamp.min, stamp.sec, micros)
end

-- Форматирует целое число из QLua в десятичной записи без экспоненты.
local function integer_text(value)
    if math.type and math.type(value) == "integer" then
        return tostring(value)
    end
    return string.format("%.0f", value)
end

-- Возвращает TSV-строку сделки trade с заранее отформатированной меткой timestamp.
local function encode_trade(trade, timestamp)
    local price = string.format("%.17g", trade.price):gsub(",", ".")
    return table.concat({trade.class_code, trade.sec_code, timestamp, integer_text(trade.trade_num),
        price, integer_text(trade.qty), integer_text(trade.flags)}, "\t")
end

-- Возвращает подпись строки таблицы, чтобы заметить сброс или замену её содержимого.
local function trade_key(trade)
    if trade then
        return table.concat({trade.class_code, trade.sec_code, integer_text(trade.trade_num),
            trade.datetime and trade_timestamp(trade.datetime) or ""}, "\t")
    end
end

-- Записывает строки rows через промежуточный файл в spool с префиксом run_id.
-- Возвращает true при успехе или nil и сообщение об ошибке при сбое.
local function publish(spool, run_id, rows)
    local base
    repeat
        sequence = sequence + 1
        base = spool .. run_id .. "-" .. string.format("%09d", sequence)
        local existing = io.open(base .. ".tsv", "rb") or io.open(base .. ".part", "rb")
        if not existing then
            break
        end
        existing:close()
    until false

    local temporary = base .. ".part"
    local file, open_error = io.open(temporary, "wb")
    if not file then
        return nil, open_error
    end
    local written, write_error = file:write(table.concat(rows, "\n"), "\n")
    local closed, close_error = file:close()
    if written and closed then
        local renamed, rename_error = os.rename(temporary, base .. ".tsv")
        if renamed then
            return true
        end
        write_error = rename_error
    end
    os.remove(temporary)
    return nil, write_error or close_error
end

-- Читает не более SCAN_SIZE элементов таблицы для доступных семейств progress.
-- Возвращает состояние курсоров в progress и пакет строк, либо nil при сбросе таблицы.
local function scan(progress, total, contracts, known, started_at)
    local first = total
    for _, cursor in pairs(progress) do
        first = math.min(first, cursor.index)
    end
    local rows = {}
    for index = first, math.min(total, first + SCAN_SIZE) - 1 do
        local trade = getItem("all_trades", index)
        if not trade then
            return nil
        end
        local root = trade.class_code == CLASS_CODE and contract_root(trade.sec_code)
        local cursor = progress[root]
        if cursor and not cursor.waiting and index >= cursor.index and trade.datetime then
            local timestamp = trade_timestamp(trade.datetime)
            if timestamp >= started_at then
                local front = front_contract(contracts[root], timestamp)
                if not front or not known[root][trade.sec_code] then
                    cursor.waiting = true
                elseif front == trade.sec_code then
                    rows[#rows + 1] = encode_trade(trade, timestamp)
                end
            end
        end
        for _, active in pairs(progress) do
            if not active.waiting and index >= active.index then
                active.index = index + 1
                active.last_trade = trade
            end
        end
    end
    return rows
end

-- Читает новые сделки и сохраняет позицию каждого семейства после публикации.
function main()
    local spool = getScriptPath() .. "\\spool\\"
    local started_at = os.date("!%Y-%m-%dT%H:%M:%S", os.time() + 3 * 3600) .. ".000000"
    local run_id = os.date("!%Y%m%d%H%M%S") .. "-" .. tostring(main):match("%x+$")
    local known = {RI = {}, MX = {}}
    local initial = getNumberOf("all_trades")
    local anchor = initial > 0 and trade_key(getItem("all_trades", initial - 1))
    local cursors = {RI = {index = initial, anchor = anchor}, MX = {index = initial, anchor = anchor}}
    local refresh_at = 0
    local contracts, catalog_ready
    local last_error
    local final_count, stop_deadline

    while true do
        local now = os.time()
        local total = getNumberOf("all_trades")
        if not running then
            final_count = final_count or total
            stop_deadline = stop_deadline or now + 4
            total = math.min(total, final_count)
        end
        if now >= refresh_at then
            contracts, catalog_ready = refresh_contracts(known)
            for root, cursor in pairs(cursors) do
                cursor.waiting = false
                if not catalog_ready[root] and not cursor.reported then
                    message(root .. ": ожидание дат экспирации в справочнике SPBFUT", 2)
                end
                cursor.reported = not catalog_ready[root]
            end
            refresh_at = now + 5
        end

        local progress = {}
        for root, cursor in pairs(cursors) do
            if total < cursor.index or (cursor.anchor and
                cursor.anchor ~= trade_key(getItem("all_trades", cursor.index - 1))) then
                cursor.index = 0
                cursor.anchor = nil
            end
            if catalog_ready[root] and not cursor.waiting and cursor.index < total then
                progress[root] = {index = cursor.index}
            end
        end

        local rows = scan(progress, total, contracts, known, started_at)
        local pause = 250
        local more = false
        if rows then
            local ok, err = true, nil
            if #rows > 0 then
                ok, err = publish(spool, run_id, rows)
            end
            if ok then
                last_error = nil
                for root, result in pairs(progress) do
                    local cursor = cursors[root]
                    cursor.index = result.index
                    cursor.waiting = result.waiting
                    if result.last_trade then
                        cursor.anchor = trade_key(result.last_trade)
                    end
                    more = more or (not cursor.waiting and cursor.index < total)
                end
            else
                if err ~= last_error then
                    message("RTS/MIX: " .. tostring(err), 2)
                    last_error = err
                end
                pause = 1000
            end
        end
        if not running and final_count and (not more or now >= stop_deadline) then
            break
        end
        if running or final_count then
            sleep(more and 10 or pause)
        end
    end
end

-- Запрашивает финальное чтение таблицы и даёт до 5 секунд на завершение.
function OnStop()
    running = false
    return 5000
end
