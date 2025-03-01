import argparse
import sys

import consumer_sol


def main(tickers: list[str], sectors: list[str], firm: str) -> None:
    consumer = consumer_sol.mqConsumer("Market Watch Exchange")
    consumer.createQueue(firm)

    topics = []

    if tickers:
        [topics.append(f"*.{ticker}.*") for ticker in tickers]
    if sectors:
        [topics.append(f"*.*.{sector}") for sector in sectors]

    for topic in topics:
        consumer.bindQueueToExchange(queueName=firm, topic=topic)

    consumer.startConsuming()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process Stock Name, Price And Type."
    )

    parser.add_argument(
        "-t",
        "--tickers",
        type=str,
        help="Stock Tickers",
        required=False,
        nargs="+",
    )

    parser.add_argument(
        "-s",
        "--sectors",
        type=str,
        help="Stock Sectors",
        required=False,
        nargs="+",
    )

    parser.add_argument(
        "-f", "--firm", type=str, help="Firm Name", required=True
    )

    args = parser.parse_args()

    if not args.tickers and not args.sectors:
        raise ValueError("Both tickers and sectors cannot be NULL")

    sys.exit(main(args.tickers, args.sectors, args.firm))