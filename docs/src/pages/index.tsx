import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className="container">
        <p className={styles.eyebrow}>
          warehouse-systems · WES tier · Supporting subdomain
        </p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroLead}>
          Two aggregates, the ExceptionCase (one open case per dedup key, a
          fixed status machine, an SLA clock, a frozen copy of the source
          fact) and the ExceptionPolicy (per source type: severity, queue,
          SLA, a declarative auto-resolve rule). Six Kafka consumers turn
          the anomaly facts the fleet already publishes into owned cases;
          operators triage, assign and resolve them over REST.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/overview/introduction">
            Read the docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference">
            API Reference
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/adr">
            ADRs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for the Exception Management bounded context: the Problem Solve case lifecycle, the handling policies, the consumed anomaly facts and the exception-management event stream.">
      <HomepageHeader />
      <main>
        <section className={styles.invariant}>
          <div className="container">
            <blockquote className={styles.invariantQuote}>
              Exactly <strong>one open case</strong> exists per dedup key
              (source type, source subject, site): a repeated fact touches
              the open case — occurrence count and last-seen move — instead
              of opening another.
            </blockquote>
            <p className={styles.invariantCaption}>
              <Link to="/docs/ddd/aggregate-design-canvas">
                The ExceptionCase aggregate →
              </Link>
            </p>
          </div>
        </section>
      </main>
    </Layout>
  );
}
