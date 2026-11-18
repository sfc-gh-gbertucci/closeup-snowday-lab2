/** App title — displayed in the nav header and browser tab */
export const APP_TITLE = 'Close-Up Market Intelligence Assistant';

/** Path to the logo in /public (used in the header and as favicon) */
export const LOGO_SRC = '/icon.svg';

/**
 * Pages listed under "Go to" in the ⌘K command palette. The group is hidden
 * while this is empty. Add an entry ("/orders") for every page you create.
 */
export const NAV_ITEMS: { label: string; href: string }[] = [];

/** Starter questions shown on the empty chat screen. */
export const SUGGESTED_QUESTIONS = [
  'What are the top 10 products by sales units in the last 12 months?',
  'How is market share evolving by therapeutic class this year?',
  'Which laboratories grew the most quarter over quarter?',
];
