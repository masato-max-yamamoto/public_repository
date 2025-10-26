library(tidyverse)
library(here)
library(arrow)
library(fs)
library(baseballr)


start_date <- ymd(20200101)
end_date <- ymd(20251231)

flag_playerid <- FALSE
flag_update <- TRUE

options(timeout = 120, download.file.method = "libcurl")
agent <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" # nolint
httr::set_config(httr::user_agent(agent))

# Wrap the scrape function so a single failed week returns an empty tibble instead of stopping the whole job
safe_scrape <- possibly(
    function(start, end, players) {
        scrape_statcast_savant_pitcher(start_date = start, end_date = end) |>
            filter(pitcher %in% players)
    },
    otherwise = tibble()
)

if (flag_playerid) {
    # Get player IDs for key players
    playerids <- c(
        playerid_lookup("Ohtani", "Shohei") |> pull(mlbam_id),
        playerid_lookup("Yamamoto", "Yoshinobu") |> pull(mlbam_id),
        playerid_lookup("Darvish", "Yu") |> pull(mlbam_id),
        playerid_lookup("Sasaki", "Roki") |> pull(mlbam_id)
    )
} else {
    playerids <- c(
        660271, # Shohei Ohtani
        808967, # Yoshinobu Yamamoto
        506433, # Yu Darvish
        808963 # Roki Sasaki
    )
}

if (flag_update) {
    pitch_jp <-
        tibble(
            get_start_date = seq(start_date, end_date, by = "1 week"),
            get_end_date = lead(get_start_date) - 1
        ) |>
        mutate(data = map2(
            get_start_date, get_end_date,
            in_parallel(~ safe_scrape(
                start = .x,
                end = .y,
                players = playerids
            ), cores = 6)
        ))

    pitch_jp <- pitch_jp |>
        mutate(nrow_data = map_int(data, nrow)) |>
        filter(nrow_data > 0) |>
        unnest(data)
    name_file <- str_c("pitch_jp_", format(start_date, "%Y%m%d"), "_", format(end_date, "%Y%m%d"), ".csv")
    pitch_jp |> write_csv(here("data", name_file))
}
